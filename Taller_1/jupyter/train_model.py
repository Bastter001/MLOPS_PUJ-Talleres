# =====================================================
# MLOPS PUJ - TRAINING PIPELINE
# Palmer Penguins Classification
# Docker Volume Compatible
# =====================================================


import os
import joblib
import pandas as pd


from sklearn.model_selection import train_test_split


from sklearn.compose import ColumnTransformer


from sklearn.pipeline import Pipeline


from sklearn.preprocessing import (
    StandardScaler,
    OneHotEncoder
)


from sklearn.linear_model import LogisticRegression


from sklearn.ensemble import (
    RandomForestClassifier,
    GradientBoostingClassifier
)


from sklearn.svm import SVC



# =====================================================
# CONFIGURACIÓN
# =====================================================


DATA_PATH = "/Data_Files/dataset.csv"

MODEL_PATH = "/models"


os.makedirs(
    MODEL_PATH,
    exist_ok=True
)



print("="*60)
print("MLOPS TRAINING PIPELINE")
print("="*60)



# =====================================================
# 1. CARGA DATASET
# =====================================================


print("\nCargando dataset...")


df = pd.read_csv(

    DATA_PATH

)



print(

    "Dimensiones:",

    df.shape

)



# =====================================================
# 2. LIMPIEZA
# =====================================================


print("\nPreparando datos...")


if "rowid" in df.columns:

    df = df.drop(

        columns=["rowid"]

    )


df = df.dropna()



print(

    "Después limpieza:",

    df.shape

)



# =====================================================
# 3. VARIABLES
# =====================================================


TARGET = "species"



X = df.drop(

    columns=[TARGET]

)



y = df[TARGET]




numeric_features = [

    "bill_length_mm",

    "bill_depth_mm",

    "flipper_length_mm",

    "body_mass_g",

    "year"

]



categorical_features = [

    "island",

    "sex"

]



# =====================================================
# 4. DIVISIÓN DATOS
# =====================================================


X_train, X_test, y_train, y_test = train_test_split(


    X,

    y,


    test_size=0.2,


    random_state=42,


    stratify=y

)



print(

    "Datos entrenamiento:",

    X_train.shape

)



print(

    "Datos prueba:",

    X_test.shape

)



# =====================================================
# 5. PREPROCESAMIENTO
# =====================================================


preprocessor = ColumnTransformer(

    transformers=[


        (

            "numeric",

            StandardScaler(),

            numeric_features

        ),



        (

            "categorical",

            OneHotEncoder(

                handle_unknown="ignore"

            ),

            categorical_features

        )

    ]

)



# =====================================================
# 6. MODELOS
# =====================================================


models = {


    "logistic_regression":

    LogisticRegression(

        max_iter=1000

    ),



    "random_forest":

    RandomForestClassifier(

        n_estimators=200,

        random_state=42

    ),



    "gradient_boosting":

    GradientBoostingClassifier(

        random_state=42

    ),



    "svm":

    SVC(

        kernel="rbf",

        probability=True,

        random_state=42

    )

}



# =====================================================
# 7. NOMBRES DE GUARDADO
# =====================================================


model_files = {


    "logistic_regression":

    "logistic_regression.pkl",


    "random_forest":

    "random_forest.pkl",


    "gradient_boosting":

    "gradient_boosting.pkl",


    "svm":

    "svm.pkl"

}



# =====================================================
# 8. ENTRENAMIENTO Y GUARDADO
# =====================================================


for name, model in models.items():


    print(

        "\nEntrenando:",

        name

    )



    pipeline = Pipeline(

        steps=[


            (

                "preprocessor",

                preprocessor

            ),



            (

                "model",

                model

            )

        ]

    )



    # Entrenamiento

    pipeline.fit(

        X_train,

        y_train

    )



    # Ruta final modelo

    model_path = os.path.join(

        MODEL_PATH,

        model_files[name]

    )



    # Guardar pipeline completo

    joblib.dump(

        pipeline,

        model_path

    )



    print(

        "Modelo guardado:",

        model_path

    )



# =====================================================
# 9. VERIFICACIÓN ARCHIVOS
# =====================================================


print("\nModelos disponibles:")



print(

    os.listdir(

        MODEL_PATH

    )

)



print(

    "\nENTRENAMIENTO COMPLETADO"

)
