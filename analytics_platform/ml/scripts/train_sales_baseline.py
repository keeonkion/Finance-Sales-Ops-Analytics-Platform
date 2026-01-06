import os
import json
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from sklearn.metrics import mean_absolute_error
from sklearn.linear_model import LinearRegression

import mlflow
import mlflow.sklearn

from ml.config import setup_mlflow

load_dotenv()


def get_engine():
    # 1) Build Postgres connection from env vars (same ones dbt uses)
    PGHOST = os.getenv("PGHOST")
    PGPORT = os.getenv("PGPORT", "5432")
    PGUSER = os.getenv("PGUSER")
    PGPASSWORD = os.getenv("PGPASSWORD")
    PGDATABASE = os.getenv("PGDATABASE")
    PGSSLMODE = os.getenv("PGSSLMODE", "require")

    assert PGHOST and PGUSER and PGPASSWORD and PGDATABASE, (
        "Missing PG env vars. Export PGHOST/PGUSER/PGPASSWORD/PGDATABASE first."
    )

    conn_str = (
        f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
        f"?sslmode={PGSSLMODE}"
    )
    return create_engine(conn_str)


def main():
    engine = get_engine()

    # 2) Load training data
    sql = "select * from analytics.ml_train_sales_monthly order by region_id, ds;"
    df = pd.read_sql(sql, engine)

    # 3) Ensure ds is datetime
    df["ds"] = pd.to_datetime(df["ds"])

    # 4) Use numeric features only
    target = "y_shipped_qty_1m"
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    feature_cols = [c for c in numeric_cols if c != target]

    # Drop rows with missing features/target
    df = df.dropna(subset=feature_cols + [target]).copy()

    # 5) Sort by time and split train/test (80/20)
    df = df.sort_values("ds").reset_index(drop=True)
    cut = int(len(df) * 0.8)

    X = df[feature_cols]
    y = df[target]

    X_train, y_train = X.iloc[:cut], y.iloc[:cut]
    X_test, y_test = X.iloc[cut:], y.iloc[cut:]
    df_test = df.iloc[cut:].copy()

    # 6) MLflow setup
    tracking_uri = setup_mlflow()
    mlflow.set_experiment("phase5_baselines")

    run_name = "sales_linear_regression_baseline"
    model_name = run_name  # keep consistent

    # ✅ IMPORTANT: everything must be inside this run
    with mlflow.start_run(run_name=run_name) as run:
        # params
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("target", target)
        mlflow.log_param("features_used", len(feature_cols))
        mlflow.log_param("train_ratio", 0.8)
        mlflow.log_param("rows_total", int(len(df)))
        mlflow.log_param("rows_train", int(len(X_train)))
        mlflow.log_param("rows_test", int(len(X_test)))

        # 7) Train
        model = LinearRegression()
        model.fit(X_train, y_train)
        pred = model.predict(X_test)

        # 8) Metrics
        mae = mean_absolute_error(y_test, pred)

        # MAPE: avoid divide by zero
        denom = y_test.replace(0, pd.NA)
        mape = (abs((y_test - pred) / denom)).mean(skipna=True)

        mlflow.log_metric("mae", float(mae))
        if pd.notna(mape):
            mlflow.log_metric("mape", float(mape))

        # 9) Save predictions back to Postgres (Task 47 reuse)
        out = df_test[["ds", "region_id"]].copy()
        out["y_true"] = y_test.values
        out["y_pred"] = pred
        out["model_name"] = model_name
        out["run_id"] = run.info.run_id
        out.to_sql(
            "ml_pred_sales_1m",
            engine,
            schema="analytics",
            if_exists="replace",
            index=False
        )

        # 10) Artifacts
        pred_path = "predictions.csv"
        out.to_csv(pred_path, index=False)
        mlflow.log_artifact(pred_path)

        feat_path = "features.json"
        with open(feat_path, "w") as f:
            json.dump(feature_cols, f, indent=2)
        mlflow.log_artifact(feat_path)

        # ✅ model artifact path MUST match your batch script: runs:/<run_id>/model
        mlflow.sklearn.log_model(model, artifact_path="model")

        # prints
        print("MLflow tracking_uri:", tracking_uri)
        print("Run logged:", run_name)
        print("run_id:", run.info.run_id)
        print("Rows:", len(df))
        print("Train rows:", len(X_train), "Test rows:", len(X_test))
        print("Features used:", len(feature_cols))
        print("MAE:", round(float(mae), 4))
        print("MAPE:", round(float(mape), 4) if pd.notna(mape) else None)
        print("Saved predictions to analytics.ml_pred_sales_1m")
        print("Model URI for batch:", f"runs:/{run.info.run_id}/model")


if __name__ == "__main__":
    main()