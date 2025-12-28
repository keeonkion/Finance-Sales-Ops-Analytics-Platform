#!/usr/bin/env python
"""
Task 12 - Load Inventory & Operations CSVs into PostgreSQL.

Supports fact daily partitions:
- Facts: database/mock_data/csv/daily/YYYYMMDD/*.csv

Usage:
  python database/etl/load_operations.py
  python database/etl/load_operations.py 20251221
"""

import sys
import os
from pathlib import Path

import psycopg2

host = os.environ["PGHOST"]
port = os.environ.get("PGPORT", "5432")
db   = os.environ["PGDATABASE"]
user = os.environ["PGUSER"]
pwd  = os.environ["PGPASSWORD"]
CONN_STR = f"postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"
#CONN_STR = os.environ["NEON_CONN_STR"]
SCHEMA = "analytics"

BASE_DIR = Path(__file__).resolve().parents[1]
DIM_CSV_DIR = BASE_DIR / "mock_data" / "csv"
FACT_DAILY_ROOT = DIM_CSV_DIR / "daily"

FACT_TABLES = ["factinventory", "factproduction"]

FACT_COLUMN_MAP = {
    "factinventory": [
        "datekey", "productkey", "warehousekey", "openingqty", "inboundqty", "outboundqty",
        "closingqty", "inventoryvalue", "averageagedays", "provisionamount",
    ],
    "factproduction": [
        "datekey", "productkey", "warehousekey", "producedqty", "scrapqty",
        "machinehours", "downtimehours",
    ],
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
    table_list = ", ".join(f"{SCHEMA}.{t}" for t in FACT_TABLES)
    print(f"🧹 Truncating Operations fact tables:\n  {table_list}")
    cur.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE;")


def load_fact_table(cur, table_name: str, csv_path: Path):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found for fact table: {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        rows = max(0, sum(1 for _ in f) - 1)

    columns = FACT_COLUMN_MAP[table_name]
    col_list_sql = ", ".join(columns)

    print(f"➡ Loading FACT {table_name} from {csv_path} ({rows} rows)...")

    with csv_path.open("r", encoding="utf-8") as f:
        copy_sql = f"""
            COPY {SCHEMA}.{table_name} ({col_list_sql})
            FROM STDIN WITH (FORMAT csv, HEADER true)
        """
        cur.copy_expert(copy_sql, f)

    print(f"   ✔ FACT {table_name} loaded.")


def main():
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    fact_dir = resolve_fact_dir(run_date)

    print("🔌 Connecting to PostgreSQL for Operations ETL...")
    print(f"📁 FACT dir : {fact_dir}")

    conn = get_conn()
    conn.autocommit = False

    try:
        cur = conn.cursor()
        truncate_tables(cur)

        for t in FACT_TABLES:
            load_fact_table(cur, t, fact_dir / f"{t}.csv")

        conn.commit()
        print("✅ Operations ETL completed successfully.")

    except Exception as ex:
        conn.rollback()
        print("❌ Error during Operations ETL. Transaction rolled back.", file=sys.stderr)
        print(ex, file=sys.stderr)
        raise
    finally:
        conn.close()
        print("🔚 Connection closed.")


if __name__ == "__main__":
    main()
