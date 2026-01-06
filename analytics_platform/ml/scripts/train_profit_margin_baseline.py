import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import mean_absolute_error
from sklearn.linear_model import LinearRegression

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
engine = create_engine(conn_str)

# 2) Load training data
sql = "select * from analytics.ml_train_profit_monthly order by region_id, ds;"
df = pd.read_sql(sql, engine)

# 3) Ensure ds is datetime
df["ds"] = pd.to_datetime(df["ds"])

# 4) Use numeric features only (avoid strings like 'January')
target = "y_operating_margin_pct_1m"
numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

# Remove target from features
feature_cols = [c for c in numeric_cols if c != target]

# Drop rows with missing values in features/target
df = df.dropna(subset=feature_cols + [target]).copy()

# 5) Sort by time and split train/test (80/20)
df = df.sort_values("ds").reset_index(drop=True)
cut = int(len(df) * 0.8)

X = df[feature_cols]
y = df[target]

X_train, y_train = X.iloc[:cut], y.iloc[:cut]
X_test, y_test = X.iloc[cut:], y.iloc[cut:]
df_test = df.iloc[cut:].copy()

# 6) Train baseline model
model = LinearRegression()
model.fit(X_train, y_train)

pred = model.predict(X_test)

# 7) Evaluate
mae = mean_absolute_error(y_test, pred)

# MAPE: avoid divide by zero
denom = y_test.replace(0, pd.NA)
mape = (abs((y_test - pred) / denom)).mean(skipna=True)

print("Rows:", len(df))
print("Train rows:", len(X_train), "Test rows:", len(X_test))
print("Features used:", len(feature_cols))
print("MAE:", round(float(mae), 4))
print("MAPE:", round(float(mape), 4) if pd.notna(mape) else None)

# 8) Save predictions back to Postgres (for Task 47 later)
out = df_test[["ds", "region_id"]].copy()
out["y_true"] = y_test.values
out["y_pred"] = pred
out["model_name"] = "linear_regression_baseline"
out.to_sql("ml_pred_profit_margin_1m", engine, schema="analytics", if_exists="replace", index=False)
print("Saved predictions to analytics.ml_pred_profit_margin_1m")
