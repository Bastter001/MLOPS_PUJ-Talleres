def guardar_metricas():
    import pandas as pd
    import pymysql

    metricas = pd.DataFrame({
        "modelo": ["RandomForest"],
        "accuracy": [0.95]
    })

    conn = pymysql.connect(
        host="10.43.97.91",
        user="airflow",
        password="Airflow_1",
        database="mlops_data"
    )

    metricas.to_sql(
        "model_metrics",
        conn,
        if_exists="replace",
        index=False
    )
