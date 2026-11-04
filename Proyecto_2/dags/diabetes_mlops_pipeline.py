from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def pending_step(step_name):
    raise NotImplementedError(
        f"La tarea {step_name} todavía debe "
        "conectarse con el módulo correspondiente"
    )


default_args = {
    "owner": "mlops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="diabetes_mlops_pipeline",
    description="Pipeline completo de diabetes MLOps",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["diabetes", "mlops"]
) as dag:

    task_names = [
        "validate_source",
        "ingest_batch",
        "validate_quality",
        "preprocess",
        "save_clean",
        "split_dataset",
        "train_models",
        "register_mlflow",
        "compare_models",
        "promote_champion"
    ]

    previous_task = None

    for name in task_names:

        task = PythonOperator(
            task_id=name,
            python_callable=pending_step,
            op_kwargs={"step_name": name}
        )

        if previous_task:
            previous_task >> task

        previous_task = task
