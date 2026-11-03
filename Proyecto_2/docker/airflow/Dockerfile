
FROM apache/airflow:3.2.2

USER airflow

COPY --chown=airflow:0 \
    docker/airflow/requirements.txt \
    /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.2.2" \
    -r /tmp/requirements.txt \
    && python -m pip check \
    && python -c "import airflow, mlflow, sklearn, pandas, boto3, psycopg2; from sqlalchemy.pool import FallbackAsyncAdaptedQueuePool; print('Dependencias verificadas')"

COPY --chown=airflow:0 \
    dags/ /opt/airflow/dags/

COPY --chown=airflow:0 \
    src/ /opt/airflow/project/src/

ENV PYTHONPATH=/opt/airflow/project
ENV MLFLOW_TRACKING_URI=http://mlflow.mlops.svc.cluster.local:5000

