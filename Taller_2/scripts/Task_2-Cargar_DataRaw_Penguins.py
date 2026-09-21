def cargar_datos():
    import pandas as pd
    import pymysql

    df = pd.read_csv('/opt/airflow/dags/penguins.csv')

    conn = pymysql.connect(
        host="10.43.97.91",
        user="airflow",
        password="Airflow_1",
        database="mlops_data"
    )

    df.to_sql(
        "penguins_raw",
        conn,
        if_exists="replace",
        index=False
    )
