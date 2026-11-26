FROM bastter/airflow-mlops:3.2.2-v3

ARG GIT_SHA=unknown

LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.title="Proyecto 3 MLOps Airflow"

ENV GIT_COMMIT=${GIT_SHA}
ENV PYTHONPATH=/opt/airflow
ENV DATA_API_GROUP=8

USER airflow

COPY --chown=airflow:0 requirements.txt \
    /tmp/proyecto3-requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/proyecto3-requirements.txt \
    && pip check

COPY --chown=airflow:0 dags/ /opt/airflow/dags/

COPY --chown=airflow:0 src/ /opt/airflow/src/

COPY --chown=airflow:0 config/ /opt/airflow/proyecto3-config/


COPY --chown=airflow:0 tests/ /opt/airflow/proyecto3-tests/

RUN python -m unittest discover \
    -s /opt/airflow/proyecto3-tests \
    -p 'test_*.py' -v

RUN python -m compileall -q \
    /opt/airflow/src \
    /opt/airflow/dags

