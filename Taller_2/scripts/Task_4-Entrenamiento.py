def entrenar():
    import pandas as pd
    import pickle
    import pymysql

    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split

    conn = pymysql.connect(
        host="10.43.97.91",
        user="airflow",
        password="Airflow_1",
        database="mlops_data"
    )

    df = pd.read_sql(
        "SELECT * FROM penguins_processed",
        conn
    )

    X = df[["bill_length_mm", "body_mass_g"]]
    y = df["species"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.3,
        random_state=42
    )

    model = RandomForestClassifier()

    model.fit(X_train, y_train)

    with open(
        "/opt/airflow/dags/model.pkl",
        "wb"
    ) as f:
        pickle.dump(model, f)
