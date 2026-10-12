FROM python:3.11-slim

RUN pip install --no-cache-dir \
    mlflow==3.16.1 \
    psycopg2-binary==2.9.10 \
    boto3==1.38.18 \
    SQLAlchemy==2.0.54

EXPOSE 5000

WORKDIR /app
