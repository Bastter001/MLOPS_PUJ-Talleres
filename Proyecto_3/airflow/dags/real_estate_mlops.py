import logging

import pendulum

from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator
)
from airflow.providers.standard.operators.empty import (
    EmptyOperator
)

from src import settings
from src.audit import (
    initialize_run,
    update_run,
    finish_run,
    save_validation
)
from src.ingestion import (
    fetch_and_preserve_batch,
    verify_raw_batch
)
from src.validation import inspect_schema


logger = logging.getLogger(__name__)


def start_run(**context):

    initialize_run(
        context["run_id"],
        settings.GIT_COMMIT
    )


def fetch_batch(**context):

    return fetch_and_preserve_batch(
        context["run_id"]
    )


def store_raw(**context):

    result = context["ti"].xcom_pull(
        task_ids="fetch_batch_from_api"
    )

    verified = verify_raw_batch(
        result["batch_id"]
    )

    verified["duplicate"] = (
        result["status"] == "DUPLICATE"
    )

    logger.info(
        "RAW integrity verification: %s",
        verified
    )

    return verified


def validate_schema_task(**context):

    result = context["ti"].xcom_pull(
        task_ids="store_raw_batch"
    )

    batch_id = result["batch_id"]

    report = inspect_schema(batch_id)

    save_validation(
        context["run_id"],
        batch_id,
        "SCHEMA_INSPECTION",
        report["status"],
        report
    )

    update_run(
        context["run_id"],
        batch_id=batch_id,
        records_received=report.get(
            "record_count"
        ),
        schema_validation=report
    )

    logger.info(
        "Schema inspection: %s",
        report
    )

    return report


def close_inspection(**context):

    report = context["ti"].xcom_pull(
        task_ids="validate_schema"
    )

    update_run(
        context["run_id"],
        training_decision="SKIP_TRAINING",
        training_reason=(
            "Fase inicial de inspeccion del contrato"
        )
    )

    finish_run(
        context["run_id"],
        "INSPECTION_COMPLETE"
    )


with DAG(
    dag_id="proyecto3_real_estate_mlops",
    description=(
        "Proyecto Final MLOps - Grupo 8"
    ),
    start_date=pendulum.datetime(
        2026, 11, 26,
        tz="America/Bogota"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    is_paused_upon_creation=True,
    default_args={
        "owner": "grupo8",
        "retries": 1,
        "retry_delay": timedelta(seconds=30)
    },
    tags=[
        "proyecto3",
        "grupo8",
        "mlops",
        "real-estate"
    ]
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_run
    )

    fetch = PythonOperator(
        task_id="fetch_batch_from_api",
        python_callable=fetch_batch,
        retries=0,
        execution_timeout=timedelta(minutes=4)
    )

    store = PythonOperator(
        task_id="store_raw_batch",
        python_callable=store_raw
    )

    validate = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema_task
    )

    notify = PythonOperator(
        task_id="notify_or_log_result",
        python_callable=close_inspection
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> fetch >> store >> validate >> notify >> end
