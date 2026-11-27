import os
import json
import hashlib
import importlib
import importlib.metadata
import importlib.util

from pathlib import Path

import requests
import mlflow

from mlflow.tracking import MlflowClient
from src.database import get_connection
from src import settings


print("===== 01. IMAGE METADATA =====")

print("GIT_COMMIT:", settings.GIT_COMMIT)
print("DATA_API_GROUP:", settings.DATA_API_GROUP)

assert settings.DATA_API_GROUP == 8

assert settings.GIT_COMMIT == os.environ["EXPECTED_GIT_SHA"]

print("IMAGE_METADATA: OK")


print("\n===== 02. DEPENDENCIES =====")

packages = [
    "apache-airflow",
    "mlflow",
    "pandas",
    "numpy",
    "scikit-learn",
    "psycopg2-binary",
    "requests",
    "scipy",
    "boto3"
]

for package in packages:
    print(
        package,
        importlib.metadata.version(package)
    )

print("DEPENDENCIES: OK")


print("\n===== 03. DAG FILES =====")

dag_directory = Path("/opt/airflow/dags")

for path in sorted(dag_directory.rglob("*.py")):
    print(path)

project_dag = (
    dag_directory / "real_estate_mlops.py"
)

diabetes_dag = (
    dag_directory / "diabetes_mlops_pipeline.py"
)

assert project_dag.is_file(), "Project DAG missing"

assert diabetes_dag.is_file(), "Diabetes DAG missing"

print("DAG_FILES: OK")


print("\n===== 04. PROJECT MODULES =====")

modules = [
    "src.settings",
    "src.database",
    "src.audit",
    "src.ingestion",
    "src.validation"
]

for module in modules:
    importlib.import_module(module)
    print(module, "OK")


print("\n===== 05. DAG IMPORT =====")

spec = importlib.util.spec_from_file_location(
    "proyecto3_dag_validation",
    project_dag
)

module = importlib.util.module_from_spec(spec)

spec.loader.exec_module(module)

dag = module.dag

print("DAG_ID:", dag.dag_id)
print("TASK_COUNT:", len(dag.tasks))

assert dag.dag_id == "proyecto3_real_estate_mlops"

expected_tasks = {
    "start",
    "fetch_batch_from_api",
    "store_raw_batch",
    "validate_schema",
    "notify_or_log_result",
    "end"
}

actual_tasks = set(dag.task_ids)

assert expected_tasks == actual_tasks

for task in dag.tasks:
    print("TASK:", task.task_id)

assert dag.max_active_runs == 1

assert dag.get_task(
    "fetch_batch_from_api"
).retries == 0

print("DAG_IMPORT: OK")


print("\n===== 06. TRAINING POLICY =====")

policy_path = Path(
    "/opt/airflow/proyecto3-config/training_policy.json"
)

with policy_path.open() as f:
    policy = json.load(f)

assert policy["source_group"] == 8

print("POLICY_VERSION:", policy["policy_version"])

print(
    "POLICY_SHA256:",
    hashlib.sha256(
        policy_path.read_bytes()
    ).hexdigest()
)

print("TRAINING_POLICY: OK")


print("\n===== 07. POSTGRESQL =====")

with get_connection() as conn:
    with conn.cursor() as cur:

        cur.execute(
            "SELECT current_database(), current_user"
        )

        database, user = cur.fetchone()

        print("DATABASE:", database)
        print("USER:", user)

        assert database == "proyecto3_data"
        assert user == "proyecto3_training"

        cur.execute(
            """
            SELECT
                has_table_privilege(
                    current_user,
                    'raw_data.batches',
                    'INSERT'
                ),
                has_column_privilege(
                    current_user,
                    'raw_data.batches',
                    'raw_payload',
                    'UPDATE'
                ),
                has_table_privilege(
                    current_user,
                    'raw_data.batches',
                    'DELETE'
                ),
                has_table_privilege(
                    current_user,
                    'clean_data.properties',
                    'INSERT'
                ),
                has_table_privilege(
                    current_user,
                    'audit.training_history',
                    'INSERT'
                )
            """
        )

        permissions = cur.fetchone()

        print("RAW_INSERT:", permissions[0])
        print("RAW_MODIFY:", permissions[1])
        print("RAW_DELETE:", permissions[2])
        print("CLEAN_INSERT:", permissions[3])
        print("AUDIT_INSERT:", permissions[4])

        assert permissions == (
            True,
            False,
            False,
            True,
            True
        )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM raw_data.batches
            """
        )

        print("STORED_RAW_BATCHES:", cur.fetchone()[0])

print("POSTGRESQL: OK")


print("\n===== 08. MLFLOW =====")

mlflow.set_tracking_uri(
    settings.MLFLOW_TRACKING_URI
)

client = MlflowClient()

experiments = client.search_experiments(
    max_results=5
)

print("TRACKING_URI:", settings.MLFLOW_TRACKING_URI)
print("EXPERIMENTS_FOUND:", len(experiments))

print("MLFLOW: OK")


print("\n===== 09. DATA API HEALTH =====")

response = requests.get(
    f"{settings.DATA_API_URL}/health",
    timeout=(5, 10)
)

response.raise_for_status()

health = response.json()

print("HTTP_STATUS:", response.status_code)
print("HEALTH:", health)

assert health.get("status") == "OK"

print("DATA_API: OK")


print("\n===== VALIDATION RESULT =====")
print("AIRFLOW_IMAGE_VALIDATION: SUCCESS")

