import json
import logging
import os

import psycopg2
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from pendulum import datetime


logger = logging.getLogger(__name__)


API_URL = os.getenv(
    "DATA_API_URL",
    "http://10.43.97.110:8080/data"
)

GROUP_NUMBER = int(
    os.getenv("DATA_API_GROUP", "8")
)


@dag(
    dag_id="01_ingestion",
    description="Obtiene una porción del batch vigente desde la API y la almacena en raw.api_data",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 9, 17, tz="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mlops",
        "retries": 0,
    },
    tags=["mlops", "ingestion", "raw"],
)
def ingestion_dag():

    @task
    def request_api():
        """
        Realiza exactamente una petición a la API externa.
        Una ejecución del DAG corresponde a una petición.
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
                    "group_number": GROUP_NUMBER
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
                "Respuesta no exitosa. Status=%s Body=%s",
                response.status_code,
                response.text[:1000],
            )

            raise AirflowFailException(
                f"La API respondió HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )

        try:
            payload = response.json()

        except ValueError as exc:
            logger.error(
                "La respuesta recibida no es JSON válido: %s",
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
        Valida la estructura mínima esperada:
        {
            group_number: int,
            batch_number: int,
            data: list
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
                f"Faltan campos obligatorios: {missing_keys}"
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
                f"El grupo retornado ({group_number}) "
                f"no coincide con el solicitado ({GROUP_NUMBER})"
            )

        if not isinstance(batch_number, int):
            raise AirflowFailException(
                "batch_number no es un entero"
            )

        if not 1 <= batch_number <= 10:
            raise AirflowFailException(
                f"batch_number fuera de rango: {batch_number}"
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
                "Se detectaron filas con estructura inválida. "
                f"Índices: {invalid_rows[:10]}"
            )

        logger.info(
            "Validación correcta: group=%s batch=%s registros=%s",
            group_number,
            batch_number,
            len(data),
        )

        return payload

    @task
    def detect_batch(payload):
        """
        Obtiene y registra el batch retornado por la API.
        """

        batch_number = payload["batch_number"]

        logger.info(
            "Batch vigente detectado: %s",
            batch_number,
        )

        logger.info(
            "Grupo: %s",
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
        Inserta una respuesta completa de la API
        como un registro JSONB en raw.api_data.
        
        Solo se conserva una porción por combinación
        batch_number + group_number.

        """

        host = os.environ["MLOPS_POSTGRES_HOST"]
        port = os.environ.get(
            "MLOPS_POSTGRES_PORT",
            "5432",
        )
        database = os.environ["MLOPS_POSTGRES_DB"]
        user = os.environ["MLOPS_POSTGRES_USER"]
        password = os.environ["MLOPS_POSTGRES_PASSWORD"]

        connection = None
        cursor = None

        try:
            connection = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
            )

            cursor = connection.cursor()

            sql = """
                INSERT INTO raw.api_data
                (
                    batch_number,
                    group_number,
                    payload
                )
                VALUES (%s, %s, %s::jsonb)

                ON CONFLICT (batch_number, group_number)
                DO NOTHING

                RETURNING id, ingestion_timestamp;
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
                        "El batch %s del grupo %s ya estaba almacenado. "
                        "No se insertó un duplicado.",
                        payload["batch_number"],

            )

            return {
                "status": "already_exists",
                "batch_number": payload["batch_number"],
                "group_number": payload["group_number"],
                "records": len(payload["data"]),
               }

            logger.info(
                "Datos almacenados en raw.api_data"
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

    payload = request_api()

    validated_payload = validate_json(
        payload
    )

    detected_payload = detect_batch(
        validated_payload
    )

    insert_raw_data(
        detected_payload
    )


ingestion_dag()
