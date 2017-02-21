FROM python:3.5
RUN echo 'debconf debconf/frontend select Noninteractive' \
    | debconf-set-selections \
    && apt-get update -qq \
    && apt-get upgrade -y

ENV PYTHONUNBUFFERED 1

RUN mkdir -p /opt/memcached_operator
WORKDIR /opt/memcached_operator

COPY Pipfile \
     Pipfile.lock \
     memcached_operator.py \
     /opt/memcached_operator/

COPY memcached_operator/ \
    /opt/memcached_operator/memcached_operator/

RUN pip install pipenv \
    && pipenv install

ENTRYPOINT ["/root/.local/share/virtualenvs/memcached_operator/bin/python", \
            "memcached_operator.py"]
