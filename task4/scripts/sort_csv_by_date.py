#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import pandas as pd


def sort_csv(input_path: Path, output_path: Path, date_col: str) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read as strings to avoid parser surprises, then parse the date column explicitly
    df = pd.read_csv(input_path, dtype=str)

    if date_col not in df.columns:
        raise KeyError(f"Column '{date_col}' not found in CSV. Available: {list(df.columns)}")

    # Normalize quotes and parse ISO8601 timestamps; invalid parses become NaT
    # Some datasets may include extra quotes, so strip surrounding quotes first
    df[date_col] = df[date_col].astype(str).str.strip('"')
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True)

    # Sort ascending by date (NaT at the end to preserve rows even if invalid)
    df_sorted = df.sort_values(by=date_col, ascending=True, na_position="last")

    # Write out as CSV with the same header; keep index off
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_sorted.to_csv(output_path, index=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Sort a CSV by a datetime column ascending")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", required=True, help="Path to output CSV")
    parser.add_argument("--date-col", required=True, help="Name of the datetime column to sort by")
    args = parser.parse_args(argv)

    sort_csv(Path(args.input), Path(args.output), args.date_col)


if __name__ == "__main__":
    sys.exit(main())
