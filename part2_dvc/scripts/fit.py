# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    mean_absolute_error,
    r2_score,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)

from catboost import CatBoostRegressor

import yaml
import os
import joblib

from func import make_ordinary_col, bool_to_int

# обучение модели
def fit_model():
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)
    data = pd.read_csv("data/initial_data.csv")

    id_col = [i for i in data.columns if i.endswith("id")]

    bool_col = data.select_dtypes(include=bool).columns

    digital_col = list(
        set(data.columns)
        - set([params["target_col"]])
        - set(id_col)
        - set(bool_col)
        - set(params["cat_ordinal_col"])
        - set(params["cat_col"])
        - set(params["digital_notfilter"])
    )
    
    make_ordinary_col(data, params["cat_ordinal_col"])
    bool_to_int_fer = FunctionTransformer(bool_to_int, validate=False)

    preprocessor = ColumnTransformer(
        [
            ("binary", bool_to_int_fer, bool_col),
            ("cat", CatBoostEncoder(return_df=False), params["cat_ordinal_col"] + params["cat_col"]),
            ("num", StandardScaler(), digital_col),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    X = data.drop(params["target_col"], axis=1)
    y = data[params["target_col"]]

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "model",
                CatBoostRegressor(
                    iterations=params["iterations"],
                    learning_rate=params["learning_rate"],
                    depth=params["depth"],
                    verbose=0,
                ),
            ),
        ]
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)

    # Рассчитываем метрики
    mae = mean_absolute_error(y_test, y_pred)
    rmse = root_mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred)

    # Выводим метрики
    print(f"Средняя абсолютная ошибка (MAE): {mae}")
    print(f"Корень средней квадратичной ошибки (RMSE): {rmse}")
    print(f"MAPE: {mape*100}%")
    print(f"Коэффициент детерминации (R²): {r2}")

    os.makedirs("models", exist_ok=True)
    with open("models/fitted_model.pkl", "wb") as fd:
        joblib.dump(pipeline, fd)


if __name__ == "__main__":
    fit_model()
