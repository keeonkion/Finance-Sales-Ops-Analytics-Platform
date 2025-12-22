#!/usr/bin/env python
"""
Task 14 - Run all ETL steps in sequence:

  1) Sales (dimensions + sales/order facts)
  2) Operations (inventory + production)
  3) Finance (PL/BS/CF)

Usage (from repo root):
  python database/etl/run_all.py
  python database/etl/run_all.py 20251221
"""

import subprocess
from pathlib import Path
import sys


def run_script(path: Path, run_date: str | None):
    print(f"\n🚀 Running: {path.name}" + (f" (run_date={run_date})" if run_date else ""))

    cmd = [sys.executable, str(path)]
    if run_date:
        cmd.append(run_date)

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ Script failed: {path}", file=sys.stderr)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)


def main():
    base_dir = Path(__file__).resolve().parent
    run_date = sys.argv[1] if len(sys.argv) > 1 else None

    if run_date and (not run_date.isdigit() or len(run_date) != 8):
        raise SystemExit("run_date must be YYYYMMDD, e.g. 20251221")

    sales = base_dir / "load_sales.py"
    ops = base_dir / "load_operations.py"
    fin = base_dir / "load_finance.py"

    run_script(sales, run_date)
    run_script(ops, run_date)
    run_script(fin, run_date)

    print("\n✅ All ETL steps completed successfully.")


if __name__ == "__main__":
    main()