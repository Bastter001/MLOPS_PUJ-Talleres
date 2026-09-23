FROM python:3.12-slim


WORKDIR /workspace


RUN pip install uv


COPY requirements.txt .


RUN uv pip install --system -r requirements.txt


RUN uv pip install --system jupyterlab


EXPOSE 8080


CMD ["jupyter","lab","--ip=0.0.0.0","--port=8080","--allow-root","--no-browser"]
