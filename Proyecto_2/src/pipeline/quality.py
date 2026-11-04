"""Validaciones de calidad para los lotes RAW."""

from sqlalchemy import text
from .common import engine


def validate_batch(batch):

    if not batch.get("batch_id"):
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException(
            "No hay registros nuevos para procesar"
        )

    with engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT payload
            FROM diabetes.raw_data
            WHERE batch_id = :batch_id
            ORDER BY source_row
        """), {
            "batch_id": batch["batch_id"]
        }).scalars().all()

    if len(rows) != batch["count"]:
        raise ValueError(
            "El total almacenado no coincide "
            "con el numero de registros del lote"
        )

    missing_target = sum(
        str(row.get("readmitted", "")).strip()
        in ("", "?", "None")
        for row in rows
    )

    print("Lote:", batch["batch_id"])
    print("Registros:", len(rows))
    print("Target faltante:", missing_target)

    return {
        "batch_id": batch["batch_id"],
        "count": len(rows),
        "missing_target": missing_target
    }
