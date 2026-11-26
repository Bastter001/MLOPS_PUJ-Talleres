import json

from src import settings
from src.database import get_connection


def read_raw_payload(batch_id):

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT raw_payload
                FROM raw_data.batches
                WHERE batch_id = %s
                """,
                (batch_id,)
            )

            row = cur.fetchone()

    if not row:
        raise RuntimeError("Lote RAW no encontrado")

    return bytes(row[0])


def inspect_schema(batch_id):

    payload = read_raw_payload(batch_id)

    try:

        document = json.loads(
            payload.decode("utf-8-sig")
        )

    except (UnicodeDecodeError, json.JSONDecodeError):

        return {
            "status": "UNSUPPORTED_FORMAT",
            "payload_size": len(payload),
            "reason": "La respuesta no es JSON valido"
        }

    result = {
        "root_type": type(document).__name__
    }

    records = None

    if isinstance(document, list):

        records = document

        result["records_path"] = "$"

    elif isinstance(document, dict):

        result["root_keys"] = sorted(
            document.keys()
        )

        expected = set(
            settings.EXPECTED_COLUMNS
        )

        if expected.issubset(document.keys()):

            records = [document]
            result["records_path"] = "$"

        else:

            candidates = []

            for key, value in document.items():

                if (
                    isinstance(value, list)
                    and value
                    and isinstance(value[0], dict)
                    and expected.intersection(
                        value[0].keys()
                    )
                ):

                    candidates.append((key, value))

            if len(candidates) == 1:

                key, records = candidates[0]
                result["records_path"] = f"$.{key}"

            else:

                result["candidate_paths"] = [
                    item[0] for item in candidates
                ]

    if records is None:

        result["status"] = "FORMAT_REVIEW_REQUIRED"

        return result

    if not records:

        result["status"] = "EMPTY_BATCH"
        result["record_count"] = 0

        return result

    if not all(
        isinstance(row, dict)
        for row in records
    ):

        result["status"] = "INVALID_RECORD_STRUCTURE"

        return result

    expected = set(settings.EXPECTED_COLUMNS)

    columns = set()
    observed_types = {}
    missing_by_column = {
        key: 0 for key in expected
    }

    for record in records:

        columns.update(record.keys())

        for key in expected - record.keys():
            missing_by_column[key] += 1

        for key, value in record.items():

            observed_types.setdefault(
                key, set()
            ).add(type(value).__name__)

    missing = sorted(expected - columns)
    extra = sorted(columns - expected)

    result.update({
        "record_count": len(records),
        "columns": sorted(columns),
        "missing_columns": missing,
        "extra_columns": extra,
        "missing_by_column": missing_by_column,
        "observed_types": {
            key: sorted(types)
            for key, types in observed_types.items()
        },
        "status": (
            "SCHEMA_REVIEW_REQUIRED"
            if missing or extra or any(
                missing_by_column.values()
            )
            else "COLUMNS_OK"
        )
    })

    return result
