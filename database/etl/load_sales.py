#!/usr/bin/env python
"""
Task 11 - Load Sales-related CSVs into PostgreSQL (Neon).

Tables covered:
  Dimensions:
    - dimdate
    - dimregion
    - dimproduct
    - dimcustomer
    - dimwarehouse
    - dimsalesrep

  Facts:
    - factsales
    - factsalestarget
    - factorders
"""

import sys
from pathlib import Path

import psycopg2

# === Connection settings (same as generate_data.py) ===
CONN_STR = (
    "postgresql://neondb_owner:npg_35XuDGUeAfHV"
    "@ep-empty-sky-a9a996ke-pooler.gwc.azure.neon.tech/neondb"
    "?sslmode=require&channel_binding=require"
)

SCHEMA = "analytics"

BASE_DIR = Path(__file__).resolve().parents[1]     # /database
CSV_DIR = BASE_DIR / "mock_data" / "csv"

DIM_TABLES = [
    "dimdate",
    "dimregion",
    "dimproduct",
    "dimcustomer",
    "dimwarehouse",
    "dimsalesrep",
]

FACT_TABLES = [
    "factsales",
    "factsalestarget",
    "factorders",
]

# 对于有 identity 主键的事实表，需要指定列名列表（不包含 ID）
FACT_COLUMN_MAP = {
    "factsales": [
        "datekey",
        "customerkey",
        "productkey",
        "regionkey",
        "salesrepkey",
        "warehousekey",
        "invoicenumber",
        "invoicelineno",
        "quantity",
        "listprice",
        "discountamount",
        "netsales",
        "cogs",
        "grossmargin",
        "currency",
    ],
    "factsalestarget": [
        "datekey",
        "regionkey",
        "salesrepkey",
        "productkey",
        "targetrevenue",
        "targetquantity",
    ],
    "factorders": [
        "ordernumber",
        "orderlineno",
        "orderdatekey",
        "customerkey",
        "productkey",
        "regionkey",
        "warehousekey",
        "orderedqty",
        "requesteddeliverydate",
        "promiseddeliverydate",
        "actualshipdate",
        "shippedqty",
        "cancelledqty",
        "isontime",
        "isinfull",
    ],
}


def get_conn():
    return psycopg2.connect(CONN_STR)


def truncate_tables(cur):
    """
    先清 FAct 再清 Dim，RESTART IDENTITY 保证主键从 1 开始。
    """
    # 事实表在前，维度在后（有 CASCADE，其实顺序也不太关键）
    all_tables = FACT_TABLES + DIM_TABLES
    table_list = ", ".join(f"{SCHEMA}.{t}" for t in all_tables)
    print(f"🧹 Truncating Sales-related tables:\n  {table_list}")
    cur.execute(
        f"""
        TRUNCATE {table_list}
        RESTART IDENTITY CASCADE;
        """
    )


def load_dim_table(cur, table_name: str):
    csv_path = CSV_DIR / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found for dim table: {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)
    rows = max(0, line_count - 1)
    print(f"➡ Loading DIM {table_name} from {csv_path} ({rows} rows)...")

    with csv_path.open("r", encoding="utf-8") as f:
        copy_sql = f"""
            COPY {SCHEMA}.{table_name}
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
        """
        cur.copy_expert(copy_sql, f)

    print(f"   ✔ DIM {table_name} loaded.")


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
    print("🔌 Connecting to PostgreSQL for Sales ETL...")
    conn = get_conn()
    conn.autocommit = False

    try:
        cur = conn.cursor()

        truncate_tables(cur)

        # 先加载维度
        for t in DIM_TABLES:
            load_dim_table(cur, t)

        # 再加载事实
        for t in FACT_TABLES:
            load_fact_table(cur, t)

        conn.commit()
        print("✅ Sales ETL completed successfully.")

    except Exception as ex:
        conn.rollback()
        print("❌ Error during Sales ETL. Transaction rolled back.", file=sys.stderr)
        print(ex, file=sys.stderr)
        raise
    finally:
        conn.close()
        print("🔚 Connection closed.")


if __name__ == "__main__":
    main()
