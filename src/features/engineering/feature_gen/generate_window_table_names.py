"""
Generate window table names and optionally write CREATE TABLE SQL files using the template.
"""

import argparse
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta


def generate_window_table_name(window_size, start_month, end_month):
    # follow naming: data_window.cus_feature_{W}m_{YYMM}_{YYMM}
    return f"data_window.cus_feature_{window_size}m_{start_month}_{end_month}"


def create_window_tables(window_sizes, start_date, end_date):
    months = pd.date_range(start_date, end_date, freq="MS")
    tables = []
    for window_size in window_sizes:
        for end_month in months:
            start_month = end_month - relativedelta(months=window_size - 1)
            start_str = start_month.strftime("%y%m")
            end_str = end_month.strftime("%y%m")
            table_name = generate_window_table_name(window_size, start_str, end_str)
            tables.append(
                {"table_name": table_name, "window_size": window_size, "start_month": start_str, "end_month": end_str}
            )
    return tables


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--windows", nargs="+", type=int, default=[1, 2, 3, 4, 6, 12])
    parser.add_argument("--out", default="database/window_templates/generated_tables.txt")
    args = parser.parse_args()

    tables = create_window_tables(args.windows, args.start, args.end)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for t in tables:
            f.write(t["table_name"] + "\n")
    print(f"Generated {len(tables)} table names -> {out_path}")
