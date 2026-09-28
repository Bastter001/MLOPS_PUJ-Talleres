FROM ghcr.io/astral-sh/uv:0.4.30 AS uv

FROM python:3.7-slim

USER root

COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /app

COPY requirements.lock /tmp/requirements.lock

RUN uv pip install \
    --system \
    --python /usr/local/bin/python \
    --no-cache \
    --only-binary=:all: \
    -r /tmp/requirements.lock

COPY main.py /app/main.py

EXPOSE 8010

CMD [
    "uvicorn",
    "main:app",
    "--host",
    "0.0.0.0",
    "--port",
    "8010"
]
