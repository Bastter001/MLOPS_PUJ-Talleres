def limpiar_bd():
    import pymysql

    conn = pymysql.connect(
        host="10.43.97.91",
        user="airflow",
        password="Airflow_1",
        database="mlops_data"
    )

    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS penguins_raw")
    cursor.execute("DROP TABLE IF EXISTS penguins_processed")
    cursor.execute("DROP TABLE IF EXISTS model_metrics")

    conn.commit()
    conn.close()
