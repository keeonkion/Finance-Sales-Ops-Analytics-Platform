#!/usr/bin/env python
"""
Task 12 - Load Inventory & Operations CSVs into PostgreSQL.

Tables covered:
  Facts:
    - factinventory
    - factproduction

Assumes Dimensions (dimdate, dimproduct, dimwarehouse, dimregion, etc.)
have already been loaded by load_sales.py.
"""

import sys
from pathlib import Path

import psycopg2

CONN_STR = (
    "postgresql://neondb_owner:npg_35XuDGUeAfHV"
    "@ep-empty-sky-a9a996ke-pooler.gwc.azure.neon.tech/neondb"
    "?sslmode=require&channel_binding=require"
)
SCHEMA = "analytics"

BASE_DIR = Path(__file__).resolve().parents[1]
CSV_DIR = BASE_DIR / "mock_data" / "csv"

FACT_TABLES = [
    "factinventory",
    "factproduction",
]

FACT_COLUMN_MAP = {
    "factinventory": [
        "datekey",
        "productkey",
        "warehousekey",
        "openingqty",
        "inboundqty",
        "outboundqty",
        "closingqty",
        "inventoryvalue",
        "averageagedays",
        "provisionamount",
    ],
    "factproduction": [
        "datekey",
        "productkey",
        "warehousekey",
        "producedqty",
        "scrapqty",
        "machinehours",
        "downtimehours",
    ],
}


def get_conn():
    return psycopg2.connect(CONN_STR)


def truncate_tables(cur):
    table_list = ", ".join(f"{SCHEMA}.{t}" for t in FACT_TABLES)
    print(f"🧹 Truncating Operations fact tables:\n  {table_list}")
    cur.execute(
        f"""
        TRUNCATE {table_list}
        RESTART IDENTITY CASCADE;
        """
    )


def load_fact_table(cur, table_name: str):
    csv_path = CSV_DIR / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found for fact table: {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)
    rows = max(0, line_count - 1)

    columns = FACT_COLUMN_MAP[table_name]
    col_list_sql = ", ".join(columns)

    print(f"➡ Loading FACT {table_name} from {csv_path} ({rows} rows)...")

    with csv_path.open("r", encoding="utf-8") as f:
        copy_sql = f"""
            COPY {SCHEMA}.{table_name} ({col_list_sql})
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
        """
        cur.copy_expert(copy_sql, f)

    print(f"   ✔ FACT {table_name} loaded.")


def main():
    print("🔌 Connecting to PostgreSQL for Operations ETL...")
    conn = get_conn()
    conn.autocommit = False

    try:
        cur = conn.cursor()

        truncate_tables(cur)

        for t in FACT_TABLES:
            load_fact_table(cur, t)

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
