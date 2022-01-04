FROM python:latest
COPY src/project-next-state.py /tmp/project-next-state.py
RUN pip install PyGithub
RUN pip install --pre gql[all]
RUN pip install slackclient
RUN pip install markdown
RUN pip install html-slacker
CMD ["python", "/tmp/project-next-state.py"]
