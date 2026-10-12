FROM python:3.11-slim

RUN pip install --no-cache-dir \
    jupyterlab==4.4.2 \
    ipykernel==6.29.5 \
    mlflow==2.22.0 \
    scikit-learn==1.6.1 \
    pandas==2.2.3 \
    numpy==2.2.5 \
    sqlalchemy==2.0.40 \
    psycopg2-binary==2.9.10

RUN useradd -m -u 1000 jovyan

USER jovyan

WORKDIR /home/jovyan/work

CMD ["sh", "-c", "exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --IdentityProvider.token=\"$JUPYTER_TOKEN\""]
