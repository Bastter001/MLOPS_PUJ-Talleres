import hashlib
import logging

import requests

from requests.adapters import HTTPAdapter

from src import settings
from src.database import get_connection


logger = logging.getLogger(__name__)


def fetch_and_preserve_batch(dag_run_id):

    group = settings.DATA_API_GROUP

    if group != 8:
        raise RuntimeError(
            "Solo se permite consumir datos del grupo 8"
        )

    # Registrar la intencion antes de llamar a la API.
    # El bloqueo protege contra ejecuciones concurrentes.

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (83008,)
            )

            cur.execute(
                """
                SELECT batch_id
                FROM raw_data.batches
                WHERE dag_run_id = %s
                """,
                (dag_run_id,)
            )

            existing = cur.fetchone()

            if existing:
                return {
                    "batch_id": existing[0],
                    "status": "RECOVERED"
                }

            cur.execute(
                """
                SELECT dag_run_id, final_status
                FROM audit.training_history
                WHERE final_status IN (
                    'REQUESTING',
                    'INGESTION_UNCERTAIN'
                )
                LIMIT 1
                """
            )

            unresolved = cur.fetchone()

            if unresolved:
                raise RuntimeError(
                    "Existe una solicitud pendiente "
                    "de revision: "
                    + unresolved[0]
                )

            cur.execute(
                """
                UPDATE audit.training_history
                SET final_status = 'REQUESTING'
                WHERE dag_run_id = %s
                  AND final_status = 'RUNNING'
                """,
                (dag_run_id,)
            )

            if cur.rowcount != 1:
                raise RuntimeError(
                    "La ejecucion no esta habilitada "
                    "para solicitar un nuevo lote"
                )

    params = {
        "group_number": group
    }

    if settings.DATA_API_DAY:
        params["day"] = settings.DATA_API_DAY

    try:

        with requests.Session() as session:

            session.mount(
                "http://",
                HTTPAdapter(max_retries=0)
            )

            session.mount(
                "https://",
                HTTPAdapter(max_retries=0)
            )

            response = session.get(
                f"{settings.DATA_API_URL}/data",
                params=params,
                timeout=(
                    settings.DATA_API_CONNECT_TIMEOUT,
                    settings.DATA_API_READ_TIMEOUT
                ),
                allow_redirects=False
            )

        payload = response.content

        digest = hashlib.sha256(
            payload
        ).hexdigest()

        batch_id = f"g{group}-{digest}"

        successful_http = (
            200 <= response.status_code < 300
        )

        ingestion_status = (
            "RECEIVED"
            if successful_http
            else "HTTP_ERROR"
        )

        duplicate = False

        # Conservar el cuerpo recibido antes de
        # interpretar el contenido.

        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO raw_data.batches (
                        batch_id,
                        dag_run_id,
                        source_group,
                        raw_payload,
                        content_type,
                        payload_sha256,
                        ingestion_status
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (payload_sha256)
                    DO NOTHING
                    RETURNING batch_id
                    """,
                    (
                        batch_id,
                        dag_run_id,
                        group,
                        payload,
                        response.headers.get(
                            "Content-Type"
                        ),
                        digest,
                        ingestion_status
                    )
                )

                inserted = cur.fetchone()

                if not inserted:

                    duplicate = True

                    cur.execute(
                        """
                        SELECT batch_id, source_group
                        FROM raw_data.batches
                        WHERE payload_sha256 = %s
                        """,
                        (digest,)
                    )

                    existing = cur.fetchone()

                    if existing[1] != group:
                        raise RuntimeError(
                            "Contenido asociado a "
                            "otro grupo"
                        )

                    batch_id = existing[0]

                final_status = (
                    "RAW_STORED"
                    if successful_http
                    else "INGESTION_UNCERTAIN"
                )

                cur.execute(
                    """
                    UPDATE audit.training_history
                    SET batch_id = %s,
                        final_status = %s,
                        error_message = %s
                    WHERE dag_run_id = %s
                    """,
                    (
                        batch_id,
                        final_status,
                        None if successful_http else
                            f"HTTP {response.status_code}",
                        dag_run_id
                    )
                )

        if not successful_http:
            raise RuntimeError(
                f"Respuesta HTTP {response.status_code}"
            )

        logger.info(
            "Lote: %s; bytes: %s; HTTP: %s",
            batch_id,
            len(payload),
            response.status_code
        )

        return {
            "batch_id": batch_id,
            "status": (
                "DUPLICATE" if duplicate else "STORED"
            ),
            "payload_size": len(payload),
            "http_status": response.status_code
        }

    except Exception as exc:

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:

                    cur.execute(
                        """
                        UPDATE audit.training_history
                        SET final_status =
                            'INGESTION_UNCERTAIN',
                            error_message = %s
                        WHERE dag_run_id = %s
                          AND final_status IN (
                              'REQUESTING',
                              'INGESTION_UNCERTAIN'
                          )
                        """,
                        (
                            type(exc).__name__,
                            dag_run_id
                        )
                    )

        except Exception:
            logger.error(
                "No fue posible actualizar auditoria"
            )

        raise


def verify_raw_batch(batch_id):

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    source_group,
                    raw_payload,
                    payload_sha256,
                    content_type,
                    ingestion_status
                FROM raw_data.batches
                WHERE batch_id = %s
                """,
                (batch_id,)
            )

            row = cur.fetchone()

    if not row:
        raise RuntimeError("Lote RAW no encontrado")

    group, payload, digest, content_type, status = row

    if group != 8:
        raise RuntimeError("Grupo de origen incorrecto")

    payload = bytes(payload)

    actual = hashlib.sha256(payload).hexdigest()

    if actual != digest:
        raise RuntimeError(
            "El hash del contenido RAW no coincide"
        )

    if status == "HTTP_ERROR":
        raise RuntimeError(
            "El lote corresponde a un error HTTP"
        )

    return {
        "batch_id": batch_id,
        "source_group": group,
        "payload_size": len(payload),
        "content_type": content_type,
        "sha256": digest,
        "integrity": "OK"
    }
