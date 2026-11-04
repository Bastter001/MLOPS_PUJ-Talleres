"""Limpieza determinista de los datos clinicos."""
import json

from sqlalchemy import text
from .common import engine


MISSING = {"", "?", "NA", "N/A", "NULL", "None"}

NUMERIC = {
    "time_in_hospital",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_outpatient",
    "number_emergency",
    "number_inpatient",
    "number_diagnoses"
}

EXCLUDE = {
    "readmitted",
    "encounter_id",
    "patient_nbr"
}

LABELS = {
    "<30": 1,
    ">30": 0,
    "NO": 0
}


def normalize_record(raw, raw_id):

    data = {
        key: (
            None
            if value is None or str(value).strip() in MISSING
            else str(value).strip()
        )
        for key, value in raw.items()
    }

    original_target = data.get("readmitted")

    if original_target not in LABELS \
            and original_target is not None:
        raise ValueError(
            f"Categoria de readmitted desconocida: "
            f"{original_target}"
        )

    target = LABELS.get(original_target)

    features = {}

    for column, value in data.items():

        if column in EXCLUDE:
            continue

        if column in NUMERIC:
            try:
                features[column] = (
                    float(value)
                    if value is not None
                    else None
                )
            except (ValueError, TypeError):
                features[column] = None

        else:
            features[column] = value

    status = (
        "ready"
        if target is not None
        else "excluded"
    )

    return {
        "raw_id": raw_id,
        "encounter_id": data.get("encounter_id"),
        "patient_id": (
            data.get("patient_nbr")
            or f"unknown-patient-{raw_id}"
        ),
        "target": target,
        "status": status,
        "reason": (
            None
            if status == "ready"
            else "missing_target"
        ),
        "features": features
    }


def preprocess_batch(quality):

    batch_id = quality["batch_id"]

    with engine().begin() as conn:

        rows = conn.execute(text("""
            SELECT raw_id, payload
            FROM diabetes.raw_data
            WHERE batch_id = :batch_id
            ORDER BY source_row
        """), {
            "batch_id": batch_id
        }).all()

        known_encounters = set(
            conn.execute(text("""
                SELECT encounter_id
                FROM diabetes.clean_data
                WHERE encounter_id IS NOT NULL
                AND batch_id <> :batch_id
            """), {
                "batch_id": batch_id
            }).scalars().all()
        )

        for raw_id, payload in rows:

            result = normalize_record(
                payload, raw_id
            )

            encounter = result["encounter_id"]

            if encounter is not None:

                if encounter in known_encounters:
                    result["status"] = "excluded"
                    result["reason"] = "duplicate_encounter_id"
                    result["target"] = None
                else:
                    known_encounters.add(encounter)

            conn.execute(text("""
                INSERT INTO diabetes.clean_stage
                (
                    raw_id, batch_id, encounter_id,
                    patient_id, target, record_status,
                    reason, features
                )
                VALUES
                (
                    :raw_id, :batch_id, :encounter_id,
                    :patient_id, :target, :record_status,
                    :reason, CAST(:features AS jsonb)
                )
                ON CONFLICT(raw_id)
                DO UPDATE SET
                    target = EXCLUDED.target,
                    record_status = EXCLUDED.record_status,
                    reason = EXCLUDED.reason,
                    features = EXCLUDED.features
            """), {
                "raw_id": raw_id,
                "batch_id": batch_id,
                "encounter_id": result["encounter_id"],
                "patient_id": result["patient_id"],
                "target": result["target"],
                "record_status": result["status"],
                "reason": result["reason"],
                "features": json.dumps(result["features"])
            })

    return {
        "batch_id": batch_id,
        "staged_rows": len(rows)
    }


def persist_clean(staging):

    batch_id = staging["batch_id"]

    with engine().begin() as conn:

        conn.execute(text("""
            INSERT INTO diabetes.clean_data
            (
                raw_id, batch_id, encounter_id,
                patient_id, target, record_status,
                reason, features
            )
            SELECT
                raw_id, batch_id, encounter_id,
                patient_id, target, record_status,
                reason, features
            FROM diabetes.clean_stage
            WHERE batch_id = :batch_id
            ON CONFLICT(raw_id)
            DO UPDATE SET
                target = EXCLUDED.target,
                record_status = EXCLUDED.record_status,
                reason = EXCLUDED.reason,
                features = EXCLUDED.features
        """), {
            "batch_id": batch_id
        })

        total = conn.execute(text("""
            SELECT COUNT(*)
            FROM diabetes.clean_data
            WHERE batch_id = :batch_id
        """), {
            "batch_id": batch_id
        }).scalar_one()

        if total != staging["staged_rows"]:
            raise RuntimeError(
                "No se almacenaron todos los registros CLEAN"
            )

        conn.execute(text("""
            UPDATE diabetes.ingestion_batches
            SET status = 'cleaned'
            WHERE batch_id = :batch_id
        """), {
            "batch_id": batch_id
        })

        conn.execute(text("""
            UPDATE diabetes.raw_data
            SET record_status = 'cleaned'
            WHERE batch_id = :batch_id
        """), {
            "batch_id": batch_id
        })

    return {
        "batch_id": batch_id,
        "clean_rows": total
    }
