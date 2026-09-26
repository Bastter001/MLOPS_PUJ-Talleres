FROM python:3.12-slim


WORKDIR /workspace


RUN pip install uv


RUN uv pip install --system \
    jupyterlab \
    pandas \
    numpy \
    scikit-learn \
    joblib


COPY jupyter .


EXPOSE 8888


CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
