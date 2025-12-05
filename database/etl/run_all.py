#!/usr/bin/env python
"""
Task 14 - Run all ETL steps in sequence:

  1) Sales (dimensions + sales/order facts)
  2) Operations (inventory + production)
  3) Finance (PL/BS/CF)

Usage (from repo root):
    python database/etl/run_all.py
"""

import subprocess
from pathlib import Path
import sys


def run_script(path: Path):
    print(f"\n🚀 Running: {path}")
    result = subprocess.run(
        [sys.executable, str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"❌ Script failed: {path}", file=sys.stderr)
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)
    else:
        print(result.stdout)


def main():
    base_dir = Path(__file__).resolve().parent

    sales = base_dir / "load_sales.py"
    ops = base_dir / "load_operations.py"
    fin = base_dir / "load_finance.py"

    run_script(sales)
    run_script(ops)
    run_script(fin)

    print("\n✅ All ETL steps completed successfully.")


if __name__ == "__main__":
    main()
