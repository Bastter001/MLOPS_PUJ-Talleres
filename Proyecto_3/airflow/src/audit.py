import json

from src.database import get_connection


ALLOWED_FIELDS = {
    "batch_id",
    "records_received",
    "records_valid",
    "records_rejected",
    "accumulated_records",
    "schema_validation",
    "quality_report",
    "new_categories",
    "drift_report",
    "training_decision",
    "training_reason",
    "candidate_version",
    "production_version",
    "promotion_decision",
    "promotion_reason",
    "candidate_metrics",
    "production_metrics",
    "performance_change",
    "mlflow_run_id",
    "git_commit",
    "final_status",
    "error_message"
}


JSON_FIELDS = {
    "schema_validation",
    "quality_report",
    "new_categories",
    "drift_report",
    "candidate_metrics",
    "production_metrics",
    "performance_change"
}


def initialize_run(dag_run_id, git_commit):

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO audit.training_history (
                    dag_run_id,
                    git_commit,
                    final_status
                )
                VALUES (%s, %s, 'RUNNING')
                ON CONFLICT (dag_run_id)
                DO NOTHING
                """,
                (dag_run_id, git_commit)
            )


def update_run(dag_run_id, **fields):

    if not fields:
        return

    unknown = set(fields) - ALLOWED_FIELDS

    if unknown:
        raise ValueError(
            f"Campos de auditoria no permitidos: {unknown}"
        )

    assignments = []
    values = []

    for key, value in fields.items():

        if key in JSON_FIELDS:
            assignments.append(f"{key} = %s::jsonb")
            values.append(
                json.dumps(value)
                if value is not None else None
            )

        else:
            assignments.append(f"{key} = %s")
            values.append(value)

    values.append(dag_run_id)

    query = (
        "UPDATE audit.training_history SET "
        + ", ".join(assignments)
        + " WHERE dag_run_id = %s"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(query, values)

            if cur.rowcount != 1:
                raise RuntimeError(
                    "No se encontro la ejecucion"
                )


def finish_run(dag_run_id, status):

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE audit.training_history
                SET final_status = %s,
                    finished_at = NOW()
                WHERE dag_run_id = %s
                """,
                (status, dag_run_id)
            )


def save_validation(
    dag_run_id,
    batch_id,
    validation_type,
    validation_status,
    details
):

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO audit.validation_reports (
                    dag_run_id,
                    batch_id,
                    validation_type,
                    validation_status,
                    details
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (
                    dag_run_id,
                    batch_id,
                    validation_type
                )
                DO UPDATE SET
                    validation_status =
                        EXCLUDED.validation_status,
                    details = EXCLUDED.details
                """,
                (
                    dag_run_id,
                    batch_id,
                    validation_type,
                    validation_status,
                    json.dumps(details)
                )
            )
