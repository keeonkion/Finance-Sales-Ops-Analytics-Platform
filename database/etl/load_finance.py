#!/usr/bin/env python
import sys
import os
from pathlib import Path

import psycopg2
import pandas as pd

host = os.environ["PGHOST"]
port = os.environ.get("PGPORT", "5432")
db   = os.environ["PGDATABASE"]
user = os.environ["PGUSER"]
pwd  = os.environ["PGPASSWORD"]

CONN_STR = f"postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"
#CONN_STR = os.environ["NEON_CONN_STR"]
SCHEMA = "analytics"

BASE_DIR = Path(__file__).resolve().parents[1]  # /database
DIM_CSV_DIR = BASE_DIR / "mock_data" / "csv"
FACT_DAILY_ROOT = DIM_CSV_DIR / "daily"

DIM_TABLE = "dimglaccount"

FACT_TABLES = ["factfinancepl", "factfinancebs", "factfinancecf"]

FACT_COLUMN_MAP = {
    "factfinancepl": ["datekey", "glaccountkey", "regionkey", "amount", "currency"],
    "factfinancebs": ["datekey", "glaccountkey", "regionkey", "balanceamount", "currency"],
    "factfinancecf": ["datekey", "glaccountkey", "regionkey", "cashflowamount", "currency"],
}


def get_conn():
    return psycopg2.connect(CONN_STR)


def resolve_fact_dir(run_date: str | None = None) -> Path:
    if run_date:
        p = FACT_DAILY_ROOT / run_date
        if not p.exists():
            raise FileNotFoundError(f"Daily fact folder not found: {p}")
        return p

    if not FACT_DAILY_ROOT.exists():
        raise FileNotFoundError(f"Daily root not found: {FACT_DAILY_ROOT}")

    candidates = [d for d in FACT_DAILY_ROOT.iterdir() if d.is_dir() and d.name.isdigit()]
    if not candidates:
        raise FileNotFoundError(f"No daily partitions found under: {FACT_DAILY_ROOT}")

    return sorted(candidates, key=lambda x: x.name)[-1]


def truncate_tables(cur):
    table_list = ", ".join([f"{SCHEMA}.{t}" for t in FACT_TABLES] + [f"{SCHEMA}.{DIM_TABLE}"])
    print(f"🧹 Truncating Finance tables:\n  {table_list}")
    cur.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE;")


def load_dim_glaccount(cur):
    csv_path = DIM_CSV_DIR / f"{DIM_TABLE}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found for dim table: {csv_path}")

    print(f"➡ Loading DIM {DIM_TABLE} from {csv_path} ...")
    with csv_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(
            f"COPY {SCHEMA}.{DIM_TABLE} FROM STDIN WITH (FORMAT csv, HEADER true)",
            f,
        )
    print(f"   ✔ DIM {DIM_TABLE} loaded.")


def load_fact_table(cur, table_name: str, fact_dir: Path):
    csv_path = fact_dir / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found for fact table: {csv_path}")

    df = pd.read_csv(csv_path)

    if "regionkey" in df.columns:
        df["regionkey"] = pd.to_numeric(df["regionkey"], errors="coerce").astype("Int64")

    tmp_path = csv_path.with_suffix(".clean.csv")
    df.to_csv(tmp_path, index=False, na_rep="")

    columns = FACT_COLUMN_MAP[table_name]
    col_list_sql = ", ".join(columns)

    print(f"➡ Loading FACT {table_name} from {tmp_path} ...")
    with tmp_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(
            f"COPY {SCHEMA}.{table_name} ({col_list_sql}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')",
            f,
        )
    print(f"   ✔ FACT {table_name} loaded.")


def main():
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    fact_dir = resolve_fact_dir(run_date)

    print("🔌 Connecting to PostgreSQL for Finance ETL...")
    print(f"📁 DIM dir  : {DIM_CSV_DIR}")
    print(f"📁 FACT dir : {fact_dir}")

    conn = get_conn()
    conn.autocommit = False

    try:
        cur = conn.cursor()

        truncate_tables(cur)
        load_dim_glaccount(cur)

        for t in FACT_TABLES:
            load_fact_table(cur, t, fact_dir)

        conn.commit()
        print("✅ Finance ETL completed successfully.")

    except Exception as ex:
        conn.rollback()
        print("❌ Error during Finance ETL. Transaction rolled back.", file=sys.stderr)
        print(ex, file=sys.stderr)
        raise
    finally:
        conn.close()
        print("🔚 Connection closed.")


if __name__ == "__main__":
    main()