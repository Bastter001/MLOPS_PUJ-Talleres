from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
import boto3


def test_postgres():
    connection = psycopg2.connect(
        host="mlops-postgres",
        port=5432,
        database="forest",
        user="mlops",
        password="mlops123",
    )

    cursor = connection.cursor()

    cursor.execute("SELECT current_database(), current_user;")

    result = cursor.fetchone()

    print("PostgreSQL connection OK")
    print(result)

    cursor.close()
    connection.close()


def test_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
    )

    buckets = s3.list_buckets()

    print("MinIO connection OK")

    for bucket in buckets["Buckets"]:
        print(bucket["Name"])


with DAG(
    dag_id="00_test_infrastructure",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mlops", "test"],
) as dag:

    postgres_task = PythonOperator(
        task_id="test_postgres",
        python_callable=test_postgres,
    )

    minio_task = PythonOperator(
        task_id="test_minio",
        python_callable=test_minio,
    )

    postgres_task >> minio_task
