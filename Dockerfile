FROM python:3.6-stretch

COPY dist/felis.tar.gz /
RUN pip install docsteady.tar.gz
WORKDIR /workspace
