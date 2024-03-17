FROM python:3.12-bullseye

WORKDIR /opt/etl

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN  apt update \
     && pip install --upgrade pip \
     && pip install -r requirements.txt \
     && apt install -y vim

COPY src/etl .
COPY docker-entrypoint.sh .

RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]