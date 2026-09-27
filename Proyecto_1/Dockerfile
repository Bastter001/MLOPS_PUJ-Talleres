FROM apache/airflow:2.6.0

USER airflow

RUN pip install --no-cache-dir \
    requests \
    pandas \
    numpy \
    psycopg2-binary \
    sqlalchemy \
    scikit-learn \
    boto3 \
    joblib
