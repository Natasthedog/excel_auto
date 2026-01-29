#!/usr/bin/env python3
"""
Convert all .tsv files in a folder to .xlsx files, skipping files that exceed Excel limits.

Usage:
  python tsv_to_xlsx_safe.py "D:/path/to/folder"
  # or run with no args to use current directory
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

MAX_EXCEL_ROWS = 1_048_576   # includes header row
MAX_EXCEL_COLS = 16_384      # XFD


def count_lines_fast(path: Path) -> int:
    """Count newline characters fast in binary mode."""
    buf_size = 8 * 1024 * 1024  # 8MB
    count = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(buf_size)
            if not chunk:
                break
            count += chunk.count(b"\n")
    # If file doesn't end with newline but has content, there is still a last line.
    # We'll do a tiny check for that.
    with path.open("rb") as f:
        f.seek(0, 2)
        if f.tell() > 0:
            f.seek(-1, 2)
            last = f.read(1)
            if last != b"\n":
                count += 1
    return count


def detect_encoding(path: Path) -> str:
    """Try utf-8, then fall back to latin-1."""
    try:
        with path.open("r", encoding="utf-8") as f:
            f.readline()
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def read_header_cols(path: Path, encoding: str) -> list[str]:
    with path.open("r", encoding=encoding, errors="strict", newline="") as f:
        header = f.readline()
    # Remove trailing newline and split on tabs
    header = header.rstrip("\r\n")
    return header.split("\t") if header else []


def tsv_to_xlsx_streaming(tsv_path: Path, xlsx_path: Path, encoding: str) -> None:
    """
    Stream TSV -> XLSX using openpyxl write-only mode (memory friendly).
    """
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title="Sheet1")

    reader = pd.read_csv(
        tsv_path,
        sep="\t",
        dtype=str,
        encoding=encoding,
        keep_default_na=False,
        chunksize=50_000,  # tweak if you want
    )

    first_chunk = True
    headers: list[str] | None = None

    for chunk in reader:
        if first_chunk:
            headers = list(chunk.columns)
            ws.append(headers)
            first_chunk = False

        # Write rows
        for row in chunk.itertuples(index=False, name=None):
            # Replace None with empty string to avoid Excel weirdness
            ws.append([("" if v is None else v) for v in row])

    wb.save(xlsx_path)


def convert_folder(folder: Path) -> None:
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"Folder not found or not a directory: {folder}")

    tsv_files = sorted(folder.glob("*.tsv"))
    if not tsv_files:
        print(f"No .tsv files found in: {folder}")
        return

    out_dir = folder / "xlsx"
    out_dir.mkdir(exist_ok=True)

    report_path = folder / "conversion_report.csv"

    with report_path.open("w", newline="", encoding="utf-8") as rf:
        writer = csv.writer(rf)
        writer.writerow(["file", "status", "reason", "lines_total", "data_rows", "cols", "output"])

        for tsv_path in tsv_files:
            xlsx_path = out_dir / f"{tsv_path.stem}.xlsx"

            try:
                encoding = detect_encoding(tsv_path)

                total_lines = count_lines_fast(tsv_path)
                if total_lines <= 0:
                    writer.writerow([tsv_path.name, "SKIPPED", "Empty file", total_lines, 0, 0, ""])
                    print(f"⚠️  {tsv_path.name}: skipped (empty)")
                    continue

                header_cols = read_header_cols(tsv_path, encoding=encoding)
                cols = len(header_cols)

                data_rows = max(total_lines - 1, 0)  # subtract header

                # Check Excel limits
                if total_lines > MAX_EXCEL_ROWS:
                    writer.writerow([
                        tsv_path.name,
                        "SKIPPED",
                        f"Too many rows for Excel (max {MAX_EXCEL_ROWS:,} incl header)",
                        total_lines,
                        data_rows,
                        cols,
                        "",
                    ])
                    print(f"⛔ {tsv_path.name}: skipped (rows {total_lines:,} > {MAX_EXCEL_ROWS:,})")
                    continue

                if cols > MAX_EXCEL_COLS:
                    writer.writerow([
                        tsv_path.name,
                        "SKIPPED",
                        f"Too many columns for Excel (max {MAX_EXCEL_COLS:,})",
                        total_lines,
                        data_rows,
                        cols,
                        "",
                    ])
                    print(f"⛔ {tsv_path.name}: skipped (cols {cols:,} > {MAX_EXCEL_COLS:,})")
                    continue

                # Convert
                tsv_to_xlsx_streaming(tsv_path, xlsx_path, encoding=encoding)
                writer.writerow([tsv_path.name, "OK", "", total_lines, data_rows, cols, str(xlsx_path)])
                print(f"✅ {tsv_path.name} -> {xlsx_path.name}")

            except Exception as e:
                writer.writerow([tsv_path.name, "FAILED", repr(e), "", "", "", ""])
                print(f"❌ {tsv_path.name}: failed ({e})")

    print(f"\nDone. Output: {out_dir}")
    print(f"Report: {report_path}")


def main() -> None:
    folder = Path(sys.argv[1]).expanduser().resolve() if len(sys.argv) > 1 else Path.cwd()
    convert_folder(folder)


if __name__ == "__main__":
    main()



pip install pandas openpyxl
python tsv_to_xlsx.py "D:\path\to\your\folder"
