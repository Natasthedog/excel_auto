#!/usr/bin/env python3
"""
Convert all .tsv files in a folder to .xlsx files.

Usage:
  python tsv_to_xlsx.py "C:/path/to/folder"
  # or run without args and it will use the current folder
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def convert_folder(folder: Path) -> None:
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"Folder not found or not a directory: {folder}")

    tsv_files = sorted(folder.glob("*.tsv"))
    if not tsv_files:
        print(f"No .tsv files found in: {folder}")
        return

    out_dir = folder / "xlsx"
    out_dir.mkdir(exist_ok=True)

    converted = 0
    for tsv_path in tsv_files:
        xlsx_path = out_dir / f"{tsv_path.stem}.xlsx"

        try:
            # Read TSV (tab-separated)
            df = pd.read_csv(tsv_path, sep="\t", dtype=str, encoding="utf-8", keep_default_na=False)
        except UnicodeDecodeError:
            # Fallback if files are not UTF-8
            df = pd.read_csv(tsv_path, sep="\t", dtype=str, encoding="latin-1", keep_default_na=False)

        # Write to Excel
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")

        print(f"✅ {tsv_path.name} -> {xlsx_path.name}")
        converted += 1

    print(f"\nDone. Converted {converted} file(s). Output folder: {out_dir}")


def main() -> None:
    folder = Path(sys.argv[1]).expanduser().resolve() if len(sys.argv) > 1 else Path.cwd()
    convert_folder(folder)


if __name__ == "__main__":
    main()



pip install pandas openpyxl
python tsv_to_xlsx.py "D:\path\to\your\folder"
