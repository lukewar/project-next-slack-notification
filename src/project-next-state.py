from datetime import datetime, timedelta
from github import Github, GithubException, RateLimitExceededException, Issue, Organization
from htmlslacker import HTMLSlacker
from slack import WebClient
from slack.errors import SlackApiError
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import codecs
import json
import markdown
import os
import re
import requests
import sys
import time
import urllib

datetime_format = "%Y-%m-%dT%H:%M:%SZ"


def escape_slack_link(original):
    # https://api.slack.com/reference/surfaces/formatting#escaping
    return original.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def get_now():
    now = datetime.utcnow()
    current_time = now.strftime(datetime_format)
    return current_time


def get_state(project_dict):
    stored = {}

    query = gql(
        f"""
        query {{
            organization(login: "{project_dict['owner']['name']}") {{
                projectNext(number: {project_dict['number']}) {{

                    fields(first: 25) {{
                        nodes {{
                            name
                            settings
                            id
                        }}
                    }}

                    items(first: 100) {{
                        edges {{
                            cursor
                            node {{
                                content {{
                                    ... on Issue {{
                                        id
                                        number
                                        title
                                        url
                                        bodyUrl
                                        repository {{
                                            name
                                            nameWithOwner
                                        }}
                                        state
                                    }}
                                }}
                                fieldValues(first: 25) {{
                                    nodes {{
                                        projectField {{
                                            id
                                        }}
                                        value
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
    """
    )
    result = gql_client.execute(query)

    fields = result["organization"]["projectNext"]["fields"]["nodes"]
    # Assume 'Status' field as pivot field. TODO: make this configurable
    pivot_field = next(x for x in fields if x["name"] == "Status")
    if pivot_field is None:
        raise ValueError("No 'Status' field found")

    # Pivot field options are the column names for Projects Classic
    pivot_field_settings = json.loads(pivot_field["settings"])
    for option in pivot_field_settings["options"]:
        stored[option['id']] = {
            "id": option['id'],
            "name": option['name'],
            "issues": {},
        }

    # TODO: pagination
    items = result["organization"]["projectNext"]["items"]["edges"]
    for item in items:
        content = item["node"]["content"]
        if content is None or bool(content) is False:
            continue

        field_values = item["node"]["fieldValues"]["nodes"]
        pivot_field_value = next(x for x in field_values if x["projectField"]["id"] == pivot_field["id"])
        assigned_pivot_field_option = pivot_field_value["value"]

        item_record = {
            "id": content["id"],
            "number": content["number"],
            "url": content["url"],
            "html_url": content["bodyUrl"],
            "title": content["title"],
            "repo": content["repository"]["name"],
            "state": content["state"],
        }
        stored[assigned_pivot_field_option]["issues"][content["id"]] = item_record

    return stored


def filter_labels(issue: Issue.Issue, labels: list):
    if len(labels) == 0:
        return True
    else:
        for label in issue.labels:
            if label.name in labels:
                return True
        return False


def resolve_url(gql_client, url):
    parsed = urllib.parse.urlparse(url)
    assert parsed.scheme == 'https', "Must be a HTTPS URL"
    assert parsed.netloc == 'github.com', "Must be on github.com"
    split = parsed.path.split('/')
    assert split[-2] == 'projects', "No projects found in URL"
    project_number = split[-1]
    project_org = split[-3]

    query = gql(
        f"""
        query {{
            organization(login: "{project_org}") {{
                projectNext(number: {project_number}) {{
                    owner {{
                        ... on Organization {{
                            name
                        }}
                    }}
                    id
                    number
                    title
                    url
                }}
            }}
        }}
    """
    )
    result = gql_client.execute(query)
    print(result)
    if result["organization"]["projectNext"] is None:
        ValueError("Couldn't resolve project with URL %s" % (url))
    return result["organization"]["projectNext"]

def get_threads(last_state):
    comment_threads = {}
    for column in last_state.values():
        for k in column["issues"].values():
            if "comments" in k.keys():
                for id in k["comments"].keys():
                    comment_threads[id] = k["comments"][id]
    return comment_threads


def get_comments(project, last_state):
    if last_state is None:
        print("last_state is none, skipping")
        return {}
    issue_last_read = {}
    for column in last_state.values():
        for k in column["issues"].values():
            if "last_read" in k.keys():
                issue_last_read[k["id"]] = k["last_read"]

    issue_comments = {}
    for column in project.get_columns():
        for card in column.get_cards():
            content = card.get_content()
            if content and isinstance(content, Issue.Issue):
                print("issue %s found" % content.html_url)
                if not filter_labels(content, labels):
                    print("issue %s filtered" % content.html_url)
                    continue
                content_id = str(content.id)
                comments = []
                comments_update = []
                if content_id in issue_last_read.keys():
                    since = datetime.strptime(
                        issue_last_read[content_id], datetime_format
                    )
                    print("looking for comments since %s" % since)
                    for comment in content.get_comments(since):
                        print("found comment %s at %s" % (comment.body, comment.created_at))
                        if comment.created_at > since:
                            comments.append(comment)
                        else:
                            comments_update.append(comment)
                else:
                    print("skipping all previous comments for %s" % content.html_url)

                issue_comments[content_id] = {
                    "id": content_id,
                    "number": content.number,
                    "html_url": content.html_url,
                    "title": content.title,
                    "comments": comments,
                    "comments_update": comments_update,
                    "issue": content,
                }
    return issue_comments


def save_data(repo, project, state):
    for column in state:
        for issue in state[column]["issues"]:
            state[column]["issues"][issue]["last_read"] = get_now()

    filename = ".data/%s.json" % project.id
    i = 1
    while True:
        try:
            content = repo.get_contents(filename)
            # TODO this will probably fail on unicode.
            return repo.update_file(content.path, "Update", json.dumps(state), content.sha)
        except GithubException as e:
            if e.status == 409: # 409 (Conflict) when other runs update at the same time
                if (i <= 3):
                    print("Received 409 when pushing updates. Sleeping for %s seconds before retry %s" % (i * 5, i))
                    time.sleep(i * 5)
                    i += 1
                    continue
                else:
                    raise "Failed to update data content"
            else:
                raise


def init_data(repo, project_dict):
    filename = f".data/{project_dict['id']}.json"
    try:
        repo.get_contents(filename)
    except GithubException as e:
        if e.status == 404:
            repo.create_file(filename, "Init commit", "")
        else:
            raise e


def get_data(repo, project_dict):
    filename = f".data/{project_dict['id']}.json"
    data = repo.get_contents(filename).decoded_content.decode("utf-8")
    if data:
        return json.loads(data)


def inherit_states(current_state, last_state):
    def get_existing_comments(last_state, id):
        if last_state is None:
            return {}
        for column in last_state.values():
            if (
                id in column["issues"].keys()
                and "comments" in column["issues"][id].keys()
            ):
                return column["issues"][id]["comments"]
        return {}

    current_state = json.loads(json.dumps(current_state))
    for column in current_state.values():
        for k in column["issues"].values():
            k["comments"] = get_existing_comments(last_state, k["id"])
    return current_state


def diff_states(current_state, last_state):
    diffs = []
    current_state = json.loads(json.dumps(current_state))
    current_issues = {}
    last_issues = {}
    for column in current_state.values():
        for k in column["issues"].values():
            current_issues[k["id"]] = {"issue": k, "column": column["id"]}

    for column in last_state.values():
        for k in column["issues"].values():
            last_issues[k["id"]] = {"issue": k, "column": column["id"]}

    current_list = set((i["issue"]["id"], i["column"]) for i in current_issues.values())
    last_list = set((i["issue"]["id"], i["column"]) for i in last_issues.values())

    for diff in current_list.difference(last_list):
        issue, column = diff
        current_column = current_state[current_issues[issue]["column"]]["name"]
        if issue not in last_issues:
            diffs.append(
                {
                    "issue": current_issues[issue]["issue"],
                    "comment": "added to the board into `%s` :wave:" % (current_column),
                }
            )

        else:
            last_column = last_state[last_issues[issue]["column"]]["name"]
            diffs.append(
                {
                    "issue": current_issues[issue]["issue"],
                    "comment": "moved from `%s` :point_right: `%s`"
                    % (last_column, current_column),
                }
            )

    for diff in last_list.difference(current_list):
        issue, column = diff
        if issue not in current_issues:
            diffs.append(
                {
                    "issue": last_issues[issue]["issue"],
                    "comment": "removed from the board :broken_heart:",
                }
            )

    return diffs


def get_env_var_name(name):
    if "LOCAL_DEV" in os.environ:
        return name
    else:
        return "INPUT_%s" % name


def get_env_var(name):
    return os.getenv(get_env_var_name(name))

def is_env_var_present(name):
    return get_env_var_name(name) in os.environ and get_env_var(name) != ""


def send_slack(project, text, attachment=None, color="#D3D3D3"):  # grey-ish
    if attachment is None:
        print(text)
        footer = "Updated in project <%s|%s>" % (project.html_url, escape_slack_link(project.name))
        attachment = {
            "mrkdwn_in": ["text"],
            "color": color,
            "text": text,
            "footer": footer,
        }

    if use_slack_api:
        response = slack.chat_postMessage(
            channel=channel, attachments=[attachment]
        )
        print("...sent to channel %s" % channel)
        return response
    else:
        body = {
            "attachments": [attachment],
        }
        response = requests.post(slack_webhook, json=body)
        print("...sent to webhook")
        return None


def convert_to_slack_markdown(gh_text):
    html = markdown.markdown(gh_text)
    # later convert back to \n
    html = html.replace("\n", "<br>")
    # slack treat header as bold
    html = re.sub(r"<h[1-6]{1}>", "<br><strong>", html)
    html = re.sub(r"</h[1-6]{1}>", "</strong>", html)
    # task list
    html = html.replace("[ ] ", "☐ ")
    html = html.replace("[x] ", "☑︎ ")
    # convert to slack markdown
    slack_markdown = HTMLSlacker(html).get_output()
    return slack_markdown


def publish_comment(text, context):
    print(text)
    print(context)
    print("---------GH_to_Slack--------")
    slack_text = convert_to_slack_markdown(text)
    print(slack_text)
    print("---------end--------")
    attachments = {
        "mrkdwn_in": ["text"],
        "color": "#D3D3D3",  # grey-ish
        "text": slack_text,
        "footer": context,
    }
    return send_slack(project, text, attachments)


def update_comment(ts, text, context):
    if not use_slack_api:
        print >> sys.stderr, "Slack Incoming Webhooks don't allow updating messages, only posting new messages is possible. Configure Slack API (SLACK_TOKEN & CHANNEL) for messages updates."
        sys.exit(1)

    print(text)
    print(context)
    print("---------GH_to_Slack--------")
    slack_text = convert_to_slack_markdown(text)
    print(slack_text)
    print("---------end--------")
    try:
        attachments = {
            "mrkdwn_in": ["text"],
            "color": "#D3D3D3",  # grey-ish
            "text": slack_text,
            "footer": context,
        }
        slack.chat_update(
            channel=channel, ts=ts, attachments=[attachments]
        )
    except SlackApiError as e:
        if e.response["error"] == "channel_not_found":
            slack.chat_postMessage(
                channel=channel,
                text=":warning: please use ID for CHANNEL (e.g. CXXXXXXXXXX) as it's required for syncing edits.",
            )
        else:
            raise e


def main(repo, project_dict):
    init_data(repo, project_dict)

    # Now do stuff.
    last_state = get_data(repo, project_dict)
    current_state = get_state(project_dict)
    current_state = inherit_states(current_state, last_state)

    if get_env_var("TRACK_ISSUES").lower() == 'true':
        comments = get_comments(project, last_state)
        for issue in comments.keys():
            for comment in comments[issue]["comments"]:
                context = "*%s* commented on <%s|%s>" % (
                    comment.user.login,
                    comment.html_url,
                    escape_slack_link(comments[issue]["title"]),
                )
                response = publish_comment(comment.body, context)
                if response is not None:
                    for column in current_state.values():
                        for k in column["issues"].values():
                            if k["id"] == issue:
                                k["comments"][comment.id] = response["ts"]
            for update in comments[issue]["comments_update"]:
                for column in current_state.values():
                    for k in column["issues"].values():
                        for id in k["comments"].keys():
                            if id == str(update.id):
                                context = "*%s* updated comment on <%s|%s>" % (
                                    update.user.login,
                                    update.html_url,
                                    escape_slack_link(comments[issue]["title"]),
                                )
                                update_comment(k["comments"][id], update.body, context)

    # TODO: remove
    print("Skipping save.")
    # save_data(repo, project, current_state)

    if not last_state:
        print("No last state found, exiting.")
        sys.exit()

    diffs = diff_states(current_state, last_state)
    if not diffs:
        print("No difference found, exiting.")
        sys.exit()


    msgs = []
    diffs = sorted(diffs, key=lambda k: k["comment"])
    for diff in diffs:
        issue_emoji = ":issue-closed:" if diff["issue"]["state"] == "closed" else ":issue:"
        color = (
            "#36a64f" if diff["issue"]["state"] == "closed" else "#439FE0"
        )  # green if closed, blue otherwise
        msgs.append(
            "%s <%s|%s> %s"
            % (
                issue_emoji,
                diff["issue"]["html_url"],
                escape_slack_link(diff["issue"]["title"]),
                diff["comment"],
            )
        )

    msgs = "\n".join(msgs)

    # send_slack(project, msgs, color=color)

# Get bits
use_slack_api = is_env_var_present("SLACK_TOKEN") and is_env_var_present("CHANNEL")
use_slack_webhook = is_env_var_present("SLACK_WEBHOOK")

if use_slack_api == use_slack_webhook:
    if use_slack_api is True:
        print("Both Slack API (SLACK_TOKEN & CHANNEL) and Slack Incoming Webhook (SLACK_WEBHOOK) are configured. Update configuration to use only one.")
    else:
        print("Missing Slack configuration. Please provide SLACK_TOKEN & CHANNEL if you wish to use Slack API, or SLACK_WEBHOOK if you wish to use Slack Incoming Webhook instead.")
    sys.exit(1)

if get_env_var_name("LABELS") in os.environ:
    if get_env_var("LABELS") == "":
        print("LABELS is empty string, won't filter")
        labels = []
    else:
        labels = get_env_var("LABELS").split(",")
else:
    print("LABELS not specified, won't filter")
    labels = []


slack = WebClient(token=get_env_var("SLACK_TOKEN"))
channel = get_env_var("CHANNEL")
slack_webhook = get_env_var("SLACK_WEBHOOK")

try:
    # Subject to GitHub RateLimitExceededException
    github = Github(get_env_var("PAT") or os.getenv("GITHUB_SCRIPT_TOKEN"))
    repo = github.get_repo(get_env_var("REPO_FOR_DATA"))

    transport = AIOHTTPTransport(url='https://api.github.com/graphql', headers={
                                 'Authorization': 'Bearer %s' % get_env_var("PAT")})
    # Create a GraphQL client using the defined transport
    gql_client = Client(transport=transport, fetch_schema_from_transport=True)
    project_dict = resolve_url(gql_client, get_env_var("PROJECT_URL"))

    main(repo, project_dict)
except RateLimitExceededException:
    print("Hit GitHub RateLimitExceededException. Skipping this run.")
