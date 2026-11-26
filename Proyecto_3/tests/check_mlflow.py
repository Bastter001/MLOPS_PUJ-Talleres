from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import mlflow

from mlflow.tracking import MlflowClient


TRACKING_URI = "http://mlflow:5000"

EXPERIMENT_NAME = (
    "proyecto3-grupo8-infrastructure-test"
)

mlflow.set_tracking_uri(TRACKING_URI)
mlflow.set_experiment(EXPERIMENT_NAME)

client = MlflowClient()

content = "Validacion infraestructura MLOps Grupo 8\n"

expected_hash = hashlib.sha256(
    content.encode("utf-8")
).hexdigest()

with mlflow.start_run(
    run_name="infrastructure-smoke-test"
) as run:

    run_id = run.info.run_id

    mlflow.set_tag("project", "Proyecto_3")
    mlflow.set_tag("source_group", "8")
    mlflow.set_tag("purpose", "infrastructure-test")

    mlflow.log_param("component", "mlflow-minio")
    mlflow.log_param("group_number", 8)
    mlflow.log_metric("test_success", 1.0)

    mlflow.log_text(
        content,
        "validation/check.txt"
    )

    print("RUN_ID:", run_id)

    print(
        "ARTIFACT_URI:",
        mlflow.get_artifact_uri()
    )

    with TemporaryDirectory() as tmpdir:

        downloaded_path = mlflow.artifacts.download_artifacts(
            run_id=run_id,
            artifact_path="validation/check.txt",
            dst_path=tmpdir
        )

        downloaded_content = Path(
            downloaded_path
        ).read_bytes()

        downloaded_hash = hashlib.sha256(
            downloaded_content
        ).hexdigest()

        assert downloaded_hash == expected_hash, (
            "El artefacto recuperado no coincide"
        )

        print("ARTIFACT_DOWNLOAD: OK")
        print("SHA256_VALIDATION: OK")

saved_run = client.get_run(run_id)

assert saved_run.info.status == "FINISHED"

assert saved_run.data.params["group_number"] == "8"

assert saved_run.data.metrics["test_success"] == 1.0

print("METADATA_VALIDATION: OK")
print("RUN_STATUS:", saved_run.info.status)
print("MLFLOW_INFRASTRUCTURE_TEST: SUCCESS")
