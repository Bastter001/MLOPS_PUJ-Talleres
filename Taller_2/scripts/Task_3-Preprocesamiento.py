def preprocesar():
    import pandas as pd
    import pymysql

    conn = pymysql.connect(
        host="10.43.97.91",
        user="airflow",
        password="Airflow_1",
        database="mlops_data"
    )

    df = pd.read_sql(
        "SELECT * FROM penguins_raw",
        conn
    )

    df = df.dropna()

    df["species"] = (
        df["species"]
        .astype("category")
        .cat.codes
    )

    df.to_sql(
        "penguins_processed",
        conn,
        if_exists="replace",
        index=False
    )
