FROM python:3.6-stretch as build

ADD . /src
WORKDIR /src
RUN python setup.py sdist
RUN cp dist/$(python setup.py --fullname).tar.gz dist/felis.tar.gz

FROM python:3.6-stretch

COPY --from=build /src/dist/felis.tar.gz /
RUN pip install felis.tar.gz
WORKDIR /workspace
