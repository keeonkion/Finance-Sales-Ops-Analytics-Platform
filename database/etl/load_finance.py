#!/usr/bin/env python
"""
Task 13 - Load Finance CSVs into PostgreSQL.

Tables covered:
  Dimension:
    - dimglaccount

  Facts:
    - factfinancepl
    - factfinancebs
    - factfinancecf
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

DIM_TABLE = "dimglaccount"

FACT_TABLES = [
    "factfinancepl",
    "factfinancebs",
    "factfinancecf",
]

FACT_COLUMN_MAP = {
    "factfinancepl": [
        "datekey",
        "glaccountkey",
        "regionkey",
        "amount",
        "currency",
    ],
    "factfinancebs": [
        "datekey",
        "glaccountkey",
        "regionkey",
        "balanceamount",
        "currency",
    ],
    "factfinancecf": [
        "datekey",
        "glaccountkey",
        "regionkey",
        "cashflowamount",
        "currency",
    ],
}


def get_conn():
    return psycopg2.connect(CONN_STR)


def truncate_tables(cur):
    table_list = ", ".join(
        [f"{SCHEMA}.{t}" for t in FACT_TABLES] + [f"{SCHEMA}.{DIM_TABLE}"]
    )
    print(f"🧹 Truncating Finance tables:\n  {table_list}")
    cur.execute(
        f"""
        TRUNCATE {table_list}
        RESTART IDENTITY CASCADE;
        """
    )


def load_dim_glaccount(cur):
    csv_path = CSV_DIR / f"{DIM_TABLE}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found for dim table: {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)
    rows = max(0, line_count - 1)
    print(f"➡ Loading DIM {DIM_TABLE} from {csv_path} ({rows} rows)...")

    with csv_path.open("r", encoding="utf-8") as f:
        copy_sql = f"""
            COPY {SCHEMA}.{DIM_TABLE}
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
        """
        cur.copy_expert(copy_sql, f)

    print(f"   ✔ DIM {DIM_TABLE} loaded.")


def load_fact_table(cur, table_name: str):
    import pandas as pd

    csv_path = CSV_DIR / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found for fact table: {csv_path}")

    # 读取原始 CSV
    df = pd.read_csv(csv_path)

    # ----- 关键处理：清洗 regionkey -----
    if "regionkey" in df.columns:
        # 把 2.0 / "2.0" / "2" 之类全部转成整数，
        # 非法的（比如 "nan"、空格、乱七八糟字符串）都变成 NaN
        df["regionkey"] = pd.to_numeric(df["regionkey"], errors="coerce")

        # 再把 2.0 -> 2，保持为整数；NaN 会保留为缺失
        df["regionkey"] = df["regionkey"].astype("Int64")  # pandas 可空的整数类型

    # 写回一个“干净版”临时文件（空值写成空字符串，COPY 会当成 NULL）
    tmp_path = csv_path.with_suffix(".clean.csv")
    df.to_csv(tmp_path, index=False, na_rep="")

    # 重新计算行数（方便日志）
    with tmp_path.open("r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)
    rows = max(0, line_count - 1)

    # 列顺序按我们在 FACT_COLUMN_MAP 里定义的来
    columns = FACT_COLUMN_MAP[table_name]
    col_list_sql = ", ".join(columns)

    print(f"➡ Loading FACT {table_name} from {tmp_path} ({rows} rows)...")

    with tmp_path.open("r", encoding="utf-8") as f:
        copy_sql = f"""
            COPY {SCHEMA}.{table_name} ({col_list_sql})
            FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')
        """
        cur.copy_expert(copy_sql, f)

    print(f"   ✔ FACT {table_name} loaded.")


def main():
    print("🔌 Connecting to PostgreSQL for Finance ETL...")
    conn = get_conn()
    conn.autocommit = False

    try:
        cur = conn.cursor()

        truncate_tables(cur)

        load_dim_glaccount(cur)

        for t in FACT_TABLES:
            load_fact_table(cur, t)

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
