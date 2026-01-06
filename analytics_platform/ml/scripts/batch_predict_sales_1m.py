import os
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sklearn.metrics import mean_absolute_error
import mlflow
import mlflow.sklearn

from ml.config import setup_mlflow

def get_engine():
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
    # 1) connect db + mlflow
    tracking_uri = setup_mlflow()
    engine = get_engine()
    mlflow.set_experiment("phase5_batch_predictions")

    # 2) load batch features (same table as training dataset)
    sql = "select * from analytics.ml_train_sales_monthly order by region_id, ds;"
    df = pd.read_sql(sql, engine)
    df["ds"] = pd.to_datetime(df["ds"])

    target = "y_shipped_qty_1m"
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    feature_cols = [c for c in numeric_cols if c != target]

    # IMPORTANT: batch predict only needs features; we can keep target if exists for evaluation
    df = df.dropna(subset=feature_cols).copy()
    df = df.sort_values("ds").reset_index(drop=True)

    X = df[feature_cols]

    # 3) Load model from the latest run of your baseline experiment
    # Option A (simple, robust): point directly to the most recent run in "phase5_baselines"
    client = mlflow.tracking.MlflowClient()
    exp = client.get_experiment_by_name("phase5_baselines")
    if exp is None:
        raise RuntimeError("MLflow experiment 'phase5_baselines' not found. Run training first.")

    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string="attributes.run_name = 'sales_linear_regression_baseline'",
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise RuntimeError("No run found for sales_linear_regression_baseline. Train first.")

    source_run = runs[0]
    run_id = source_run.info.run_id

    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)

    # 4) Predict
    y_pred = model.predict(X)

    # 5) Write back to Postgres
    prediction_time = datetime.now(timezone.utc).isoformat()

    out = df[["ds", "region_id"]].copy()
    out["y_pred"] = y_pred
    out["model_name"] = "sales_linear_regression_baseline"
    out["run_id"] = run_id
    out["prediction_time"] = prediction_time

    # optional evaluation if y_true exists
    if target in df.columns and df[target].notna().any():
        df_eval = df.dropna(subset=[target]).copy()
        if len(df_eval) > 0:
            mae = mean_absolute_error(df_eval[target], model.predict(df_eval[feature_cols]))
        else:
            mae = None
    else:
        mae = None

    out.to_sql("ml_pred_sales_1m", engine, schema="analytics", if_exists="replace", index=False)

    # 6) Log the batch run itself
    with mlflow.start_run(run_name="batch_predict_sales_1m"):
        mlflow.log_param("source_model_run_id", run_id)
        mlflow.log_param("model_uri", model_uri)
        mlflow.log_param("rows_scored", len(out))
        mlflow.log_param("features_used", len(feature_cols))
        if mae is not None:
            mlflow.log_metric("mae_on_available_labels", float(mae))

        tmp_csv = "batch_pred_sales_1m.csv"
        out.to_csv(tmp_csv, index=False)
        mlflow.log_artifact(tmp_csv)

    print("MLflow tracking_uri:", tracking_uri)
    print("Loaded model from run_id:", run_id)
    print("Rows scored:", len(out))
    print("Saved predictions to analytics.ml_pred_sales_1m")


if __name__ == "__main__":
    main()