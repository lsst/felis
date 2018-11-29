FROM python:3.6-stretch

COPY dist/felis.tar.gz /
RUN pip install felis.tar.gz
WORKDIR /workspace
