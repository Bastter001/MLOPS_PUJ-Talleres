import json
import logging
import os

import psycopg2
import requests

from airflow.decorators import dag, task
from airflow.exceptions import (
    AirflowFailException,
    AirflowSkipException,
)
from airflow.models import DagModel
from airflow.utils.session import create_session
from pendulum import datetime


logger = logging.getLogger(__name__)


API_URL = os.getenv(
    "DATA_API_URL",
    "http://10.43.97.110:8080/data",
)

GROUP_NUMBER = int(
    os.getenv(
        "DATA_API_GROUP",
        "8",
    )
)


def get_postgres_connection():
    """
    Crea una conexión con PostgreSQL del proyecto.

    Base esperada:
        forest

    Servicio Docker:
        mlops-postgres

    Dentro de Docker se usa el puerto 5432.
    """

    return psycopg2.connect(
        host=os.environ["MLOPS_POSTGRES_HOST"],
        port=os.environ.get(
            "MLOPS_POSTGRES_PORT",
            "5432",
        ),
        dbname=os.environ["MLOPS_POSTGRES_DB"],
        user=os.environ["MLOPS_POSTGRES_USER"],
        password=os.environ["MLOPS_POSTGRES_PASSWORD"],
    )


@dag(
    dag_id="01_ingestion",
    description=(
        "Obtiene una porción del batch vigente desde "
        "la API y la almacena en raw.api_data"
    ),
    schedule="*/5 * * * *",
    start_date=datetime(
        2026,
        9,
        1,
        tz="America/Bogota",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mlops",
        "retries": 0,
    },
    tags=[
        "mlops",
        "ingestion",
        "raw",
    ],
)
def ingestion_dag():

    @task
    def check_collection_status():
        """
        Verifica cuántos batches distintos
        ya fueron almacenados.

        Si ya existen los 10 batches,
        se evita realizar una nueva petición.
        """

        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT COUNT(DISTINCT batch_number)
                FROM raw.api_data
                WHERE group_number = %s;
                """,
                (GROUP_NUMBER,),
            )

            batch_count = cursor.fetchone()[0]

            logger.info(
                "Batches actualmente recolectados: %s/10",
                batch_count,
            )

            if batch_count >= 10:
                logger.info(
                    "Los 10 batches ya fueron recolectados. "
                    "No se realizará otra petición."
                )

                raise AirflowSkipException(
                    "Recolección completa: "
                    "ya existen los 10 batches."
                )

            return batch_count

        except psycopg2.Error as exc:
            logger.exception(
                "Error consultando el estado de la colección"
            )

            raise AirflowFailException(
                f"Error PostgreSQL: {exc}"
            ) from exc

        finally:
            if cursor:
                cursor.close()

            if connection:
                connection.close()

    @task
    def request_api():
        """
        Realiza exactamente una petición
        a la API externa.
        """

        logger.info(
            "Solicitando datos a %s para group_number=%s",
            API_URL,
            GROUP_NUMBER,
        )

        try:
            response = requests.get(
                API_URL,
                params={
                    "group_number": GROUP_NUMBER,
                },
                timeout=30,
            )

        except requests.exceptions.Timeout as exc:
            logger.exception(
                "Timeout al consultar la API"
            )

            raise AirflowFailException(
                "Timeout al consultar la API externa"
            ) from exc

        except requests.exceptions.ConnectionError as exc:
            logger.exception(
                "No fue posible conectar con la API"
            )

            raise AirflowFailException(
                "No fue posible conectar con la API externa"
            ) from exc

        except requests.exceptions.RequestException as exc:
            logger.exception(
                "Error inesperado al realizar la petición"
            )

            raise AirflowFailException(
                f"Error HTTP: {exc}"
            ) from exc

        logger.info(
            "API respondió HTTP %s",
            response.status_code,
        )

        if response.status_code != 200:
            logger.error(
                "Respuesta no exitosa. "
                "Status=%s Body=%s",
                response.status_code,
                response.text[:1000],
            )

            raise AirflowFailException(
                f"La API respondió HTTP "
                f"{response.status_code}: "
                f"{response.text[:500]}"
            )

        try:
            payload = response.json()

        except ValueError as exc:
            logger.error(
                "La respuesta recibida "
                "no es JSON válido: %s",
                response.text[:1000],
            )

            raise AirflowFailException(
                "La API no retornó un JSON válido"
            ) from exc

        logger.info(
            "JSON recibido correctamente"
        )

        return payload

    @task
    def validate_json(payload):
        """
        Valida la estructura esperada:

        {
            "group_number": int,
            "batch_number": int,
            "data": list
        }
        """

        if not isinstance(payload, dict):
            raise AirflowFailException(
                "El payload debe ser un objeto JSON"
            )

        required_keys = {
            "group_number",
            "batch_number",
            "data",
        }

        missing_keys = required_keys - payload.keys()

        if missing_keys:
            raise AirflowFailException(
                f"Faltan campos obligatorios: "
                f"{missing_keys}"
            )

        group_number = payload["group_number"]
        batch_number = payload["batch_number"]
        data = payload["data"]

        if not isinstance(group_number, int):
            raise AirflowFailException(
                "group_number no es un entero"
            )

        if group_number != GROUP_NUMBER:
            raise AirflowFailException(
                f"El grupo retornado "
                f"({group_number}) "
                f"no coincide con el solicitado "
                f"({GROUP_NUMBER})"
            )

        if not isinstance(batch_number, int):
            raise AirflowFailException(
                "batch_number no es un entero"
            )

        if not 1 <= batch_number <= 10:
            raise AirflowFailException(
                f"batch_number fuera de rango: "
                f"{batch_number}"
            )

        if not isinstance(data, list):
            raise AirflowFailException(
                "El campo data debe ser una lista"
            )

        if len(data) == 0:
            raise AirflowFailException(
                "La API retornó una lista data vacía"
            )

        invalid_rows = []

        for index, row in enumerate(data):

            if not isinstance(row, list):
                invalid_rows.append(index)
                continue

            if len(row) != 13:
                invalid_rows.append(index)

        if invalid_rows:
            raise AirflowFailException(
                "Se detectaron filas con estructura "
                f"inválida. Índices: "
                f"{invalid_rows[:10]}"
            )

        logger.info(
            "Validación correcta: "
            "group=%s batch=%s registros=%s",
            group_number,
            batch_number,
            len(data),
        )

        return payload

    @task
    def detect_batch(payload):
        """
        Obtiene el número de batch
        retornado por la API.
        """

        batch_number = payload["batch_number"]

        logger.info(
            "Batch vigente detectado: %s",
            batch_number,
        )

        logger.info(
            "Grupo detectado: %s",
            payload["group_number"],
        )

        logger.info(
            "Número de registros recibidos: %s",
            len(payload["data"]),
        )

        return payload

    @task
    def insert_raw_data(payload):
        """
        Inserta la respuesta completa
        en raw.api_data.

        Se conserva una sola porción por
        combinación batch_number + group_number.
        """

        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            sql = """
                INSERT INTO raw.api_data
                (
                    batch_number,
                    group_number,
                    payload
                )
                VALUES (%s, %s, %s::jsonb)

                ON CONFLICT
                (
                    batch_number,
                    group_number
                )
                DO NOTHING

                RETURNING
                    id,
                    ingestion_timestamp;
            """

            cursor.execute(
                sql,
                (
                    payload["batch_number"],
                    payload["group_number"],
                    json.dumps(payload),
                ),
            )

            inserted_row = cursor.fetchone()

            connection.commit()

            if inserted_row is None:

                logger.warning(
                    "El batch %s del grupo %s "
                    "ya estaba almacenado. "
                    "No se insertó un duplicado.",
                    payload["batch_number"],
                    payload["group_number"],
                )

                return {
                    "status": "already_exists",
                    "batch_number": payload["batch_number"],
                    "group_number": payload["group_number"],
                    "records": len(payload["data"]),
                }

            logger.info(
                "Datos almacenados correctamente "
                "en raw.api_data"
            )

            logger.info(
                "ID generado: %s",
                inserted_row[0],
            )

            logger.info(
                "Timestamp: %s",
                inserted_row[1],
            )

            logger.info(
                "Batch almacenado: %s",
                payload["batch_number"],
            )

            logger.info(
                "Grupo: %s",
                payload["group_number"],
            )

            logger.info(
                "Registros contenidos en payload: %s",
                len(payload["data"]),
            )

            return {
                "status": "inserted",
                "id": inserted_row[0],
                "batch_number": payload["batch_number"],
                "group_number": payload["group_number"],
                "records": len(payload["data"]),
            }

        except psycopg2.Error as exc:

            if connection:
                connection.rollback()

            logger.exception(
                "Error insertando datos en PostgreSQL"
            )

            raise AirflowFailException(
                f"Error PostgreSQL: {exc}"
            ) from exc

        finally:

            if cursor:
                cursor.close()

            if connection:
                connection.close()

    @task
    def finalize_collection():
        """
        Verifica si ya existen los 10 batches.

        Cuando se completan los 10,
        pausa automáticamente el DAG
        01_ingestion.
        """

        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT batch_number),
                    ARRAY_AGG(
                        DISTINCT batch_number
                        ORDER BY batch_number
                    )
                FROM raw.api_data
                WHERE group_number = %s;
                """,
                (GROUP_NUMBER,),
            )

            result = cursor.fetchone()

            batch_count = result[0]
            batches = result[1] or []

            logger.info(
                "Estado de recolección: %s/10 batches",
                batch_count,
            )

            logger.info(
                "Batches recolectados: %s",
                batches,
            )

            if batch_count < 10:

                logger.info(
                    "La recolección aún no está completa."
                )

                return {
                    "complete": False,
                    "batch_count": batch_count,
                    "batches": batches,
                }

            logger.info(
                "Los 10 batches fueron recolectados."
            )

            with create_session() as session:

                dag_model = (
                    session.query(DagModel)
                    .filter(
                        DagModel.dag_id
                        == "01_ingestion"
                    )
                    .one_or_none()
                )

                if dag_model:

                    dag_model.is_paused = True

                    session.commit()

                    logger.info(
                        "DAG 01_ingestion "
                        "pausado automáticamente."
                    )

            return {
                "complete": True,
                "batch_count": batch_count,
                "batches": batches,
            }

        except psycopg2.Error as exc:
            logger.exception(
                "Error verificando "
                "la finalización de la colección"
            )

            raise AirflowFailException(
                f"Error PostgreSQL: {exc}"
            ) from exc

        finally:

            if cursor:
                cursor.close()

            if connection:
                connection.close()

    #
    # Flujo del DAG
    #

    collection_status = check_collection_status()

    payload = request_api()

    validated_payload = validate_json(
        payload
    )

    detected_payload = detect_batch(
        validated_payload
    )

    insert_result = insert_raw_data(
        detected_payload
    )

    completion = finalize_collection()

    #
    # Dependencias explícitas adicionales
    #

    collection_status >> payload

    insert_result >> completion


ingestion_dag()
