import os
from dotenv import load_dotenv
load_dotenv()

import json
import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import mean_absolute_error
from sklearn.linear_model import LinearRegression

import mlflow
import mlflow.sklearn
from ml.config import setup_mlflow


# -------------------------------------------------
# 1) Postgres connection (same as dbt)
# -------------------------------------------------
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
engine = create_engine(conn_str)


# -------------------------------------------------
# 2) Load training data
# -------------------------------------------------
sql = """
select *
from analytics.ml_train_profit_monthly
order by region_id, ds;
"""
df = pd.read_sql(sql, engine)
df["ds"] = pd.to_datetime(df["ds"])


# -------------------------------------------------
# 3) Feature selection (numeric only)
# -------------------------------------------------
target = "y_operating_profit_1m"

numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
feature_cols = [c for c in numeric_cols if c != target]

df = df.dropna(subset=feature_cols + [target]).copy()


# -------------------------------------------------
# 4) Train / test split (time-based 80/20)
# -------------------------------------------------
df = df.sort_values("ds").reset_index(drop=True)
cut = int(len(df) * 0.8)

X = df[feature_cols]
y = df[target]

X_train, y_train = X.iloc[:cut], y.iloc[:cut]
X_test, y_test = X.iloc[cut:], y.iloc[cut:]
df_test = df.iloc[cut:].copy()


# -------------------------------------------------
# 5) MLflow setup
# -------------------------------------------------
tracking_uri = setup_mlflow()
mlflow.set_experiment("phase5_baselines")

run_name = "profit_linear_regression_baseline"


with mlflow.start_run(run_name=run_name):

    # ---- params
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("target", target)
    mlflow.log_param("features_used", len(feature_cols))
    mlflow.log_param("train_ratio", 0.8)

    # ---- train
    model = LinearRegression()
    model.fit(X_train, y_train)

    pred = model.predict(X_test)

    # ---- metrics
    mae = mean_absolute_error(y_test, pred)

    denom = y_test.replace(0, pd.NA)
    mape = (abs((y_test - pred) / denom)).mean(skipna=True)

    mlflow.log_metric("mae", float(mae))
    mlflow.log_metric("mape", float(mape))

    # ---- console output
    print("Rows:", len(df))
    print("Train rows:", len(X_train), "Test rows:", len(X_test))
    print("Features used:", len(feature_cols))
    print("MAE:", round(float(mae), 4))
    print("MAPE:", round(float(mape), 4) if pd.notna(mape) else None)

    # -------------------------------------------------
    # 6) Save predictions to Postgres
    # -------------------------------------------------
    out = df_test[["ds", "region_id"]].copy()
    out["y_true"] = y_test.values
    out["y_pred"] = pred
    out["model_name"] = "linear_regression_baseline"

    out.to_sql(
        "ml_pred_profit_1m",
        engine,
        schema="analytics",
        if_exists="replace",
        index=False,
    )

    # -------------------------------------------------
    # 7) Artifacts
    # -------------------------------------------------
    pred_path = "profit_predictions.csv"
    out.to_csv(pred_path, index=False)
    mlflow.log_artifact(pred_path)

    feat_path = "profit_features.json"
    with open(feat_path, "w") as f:
        json.dump(feature_cols, f, indent=2)
    mlflow.log_artifact(feat_path)

    mlflow.sklearn.log_model(model, name="model")

    print("MLflow tracking_uri:", tracking_uri)
    print("Run logged:", run_name)
    print("Saved predictions to analytics.ml_pred_profit_1m")