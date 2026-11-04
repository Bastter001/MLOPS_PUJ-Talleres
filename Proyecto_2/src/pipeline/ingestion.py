"""Ingesta incremental y transaccional de diabetes."""
import csv
import hashlib
import json
import os
from pathlib import Path

from sqlalchemy import text
from .common import engine


SOURCE = Path(
    os.environ.get(
        "DIABETES_CSV_PATH",
        "/opt/airflow/data/diabetic_data.csv"
    )
)


def validate_source():
    if not SOURCE.is_file():
        raise FileNotFoundError(
            f"CSV no encontrado: {SOURCE}"
        )

    with SOURCE.open(
        "r", newline="", encoding="utf-8-sig"
    ) as fp:
        fields = csv.DictReader(fp).fieldnames or []

    required = {
        "encounter_id",
        "patient_nbr",
        "readmitted"
    }

    missing = required - set(fields)

    if missing:
        raise ValueError(
            f"Columnas faltantes: {missing}"
        )

    digest = hashlib.sha256()

    with SOURCE.open("rb") as fp:
        for block in iter(
            lambda: fp.read(1024 * 1024), b""
        ):
            digest.update(block)

    return {
        "source_hash": digest.hexdigest(),
        "path": str(SOURCE)
    }


def ingest_next_batch(meta, max_rows=15000):
    if not 1 <= max_rows <= 15000:
        raise ValueError(
            "El maximo permitido es 15000 registros"
        )

    with engine().begin() as conn:

        conn.execute(text(
            "SELECT pg_advisory_xact_lock(20261104)"
        ))

        next_row = conn.execute(text("""
            SELECT COALESCE(MAX(source_row), 0) + 1
            FROM diabetes.raw_data
            WHERE source_hash = :hash
        """), {
            "hash": meta["source_hash"]
        }).scalar_one()

        records = []

        with SOURCE.open(
            "r", newline="", encoding="utf-8-sig"
        ) as fp:
            for row_num, row in enumerate(
                csv.DictReader(fp), start=1
            ):
                if row_num < next_row:
                    continue

                records.append((row_num, row))

                if len(records) >= max_rows:
                    break

        if not records:
            return {
                "batch_id": None,
                "count": 0
            }

        first = records[0][0]
        last = records[-1][0]

        batch_id = conn.execute(text("""
            INSERT INTO diabetes.ingestion_batches
            (
                source_hash,
                source_file,
                first_row,
                last_row,
                row_count
            )
            VALUES (
                :hash, :filename, :first, :last, :count
            )
            ON CONFLICT (
                source_hash, first_row, last_row
            )
            DO UPDATE SET
                source_file = EXCLUDED.source_file
            RETURNING batch_id
        """), {
            "hash": meta["source_hash"],
            "filename": SOURCE.name,
            "first": first,
            "last": last,
            "count": len(records)
        }).scalar_one()

        for row_num, row in records:
            conn.execute(text("""
                INSERT INTO diabetes.raw_data
                (
                    source_hash,
                    source_row,
                    batch_id,
                    source_file,
                    payload
                )
                VALUES (
                    :hash,
                    :row,
                    :batch,
                    :filename,
                    CAST(:payload AS jsonb)
                )
                ON CONFLICT (
                    source_hash, source_row
                )
                DO NOTHING
            """), {
                "hash": meta["source_hash"],
                "row": row_num,
                "batch": batch_id,
                "filename": SOURCE.name,
                "payload": json.dumps(row)
            })

    return {
        "batch_id": batch_id,
        "count": len(records),
        "source_hash": meta["source_hash"]
    }
