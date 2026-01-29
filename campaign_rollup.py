#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


# Excel limits (not really needed now, since aggregated output is small)
MAX_EXCEL_ROWS = 1_048_576

# Candidate column name variants (case/spacing/underscore insensitive)
COL_CANDIDATES = {
    "campaign_id": ["campaignid", "campaign_id", "campaign id", "campaign-id", "idcampaign", "campaignidentifier"],
    "campaign_name": ["campaignname", "campaign_name", "campaign name", "campaign-name", "namecampaign"],
    "spend": ["spend", "cost", "amount", "adspend", "media spend", "media_spend"],
    "impressions": ["impressions", "imps", "impr", "views"],
    "clicks": ["clicks", "click", "clk"],
    "nbobs": ["nbobs", "n_bobs", "bobs", "nb_observations", "nobs", "observations"],
}


def _norm(s: str) -> str:
    """Normalize column names for matching."""
    return "".join(ch for ch in s.strip().lower() if ch.isalnum())


def find_required_columns(columns: Iterable[str]) -> dict[str, str]:
    """Return mapping of canonical->actual column name from a header list."""
    norm_map = {_norm(c): c for c in columns}

    found: dict[str, str] = {}
    for canonical, candidates in COL_CANDIDATES.items():
        for cand in candidates:
            key = _norm(cand)
            if key in norm_map:
                found[canonical] = norm_map[key]
                break

    missing = [k for k in ["campaign_id", "campaign_name", "spend", "impressions", "clicks", "nbobs"] if k not in found]
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}\n"
            f"Available columns: {list(columns)}\n"
            f"Tip: if your file uses different headers, add aliases in COL_CANDIDATES."
        )
    return found


def coerce_number(series: pd.Series, is_money: bool = False) -> pd.Series:
    """
    Convert strings to numeric efficiently, handling common formatting.
    - For money: tries to handle commas/dots.
    """
    s = series.astype(str).str.strip()

    if is_money:
        # Heuristic:
        # - If it has both ',' and '.', assume ',' is thousands sep -> remove ','
        # - If it has ',' but no '.', assume ',' is decimal -> replace with '.'
        has_comma = s.str.contains(",", na=False)
        has_dot = s.str.contains(r"\.", na=False)
        s = s.where(~(has_comma & has_dot), s.str.replace(",", "", regex=False))
        s = s.where(~(has_comma & ~has_dot), s.str.replace(",", ".", regex=False))

        # Remove currency symbols/spaces
        s = s.str.replace("€", "", regex=False).str.replace("$", "", regex=False).str.replace(" ", "", regex=False)

    return pd.to_numeric(s, errors="coerce").fillna(0)


def rollup_tsv(tsv_path: Path, out_dir: Path, chunksize: int = 500_000) -> Path:
    """
    Stream a TSV file, group by campaign_id + campaign_name, sum metrics, export to xlsx.
    Returns output xlsx path.
    """
    # Read header only (fast) to identify exact column names and restrict usecols.
    header_df = pd.read_csv(tsv_path, sep="\t", nrows=0, encoding_errors="replace")
    colmap = find_required_columns(header_df.columns)

    usecols = [
        colmap["campaign_id"],
        colmap["campaign_name"],
        colmap["spend"],
        colmap["impressions"],
        colmap["clicks"],
        colmap["nbobs"],
    ]

    acc = None

    reader = pd.read_csv(
        tsv_path,
        sep="\t",
        usecols=usecols,
        dtype={
            colmap["campaign_id"]: "string",
            colmap["campaign_name"]: "string",
        },
        chunksize=chunksize,
        encoding_errors="replace",
        low_memory=True,
    )

    for chunk in reader:
        # Normalize key types
        chunk[colmap["campaign_id"]] = chunk[colmap["campaign_id"]].fillna("").astype("string")
        chunk[colmap["campaign_name"]] = chunk[colmap["campaign_name"]].fillna("").astype("string")

        # Coerce numerics
        chunk[colmap["spend"]] = coerce_number(chunk[colmap["spend"]], is_money=True)
        for c in [colmap["impressions"], colmap["clicks"], colmap["nbobs"]]:
            chunk[c] = coerce_number(chunk[c], is_money=False)

        grouped = (
            chunk.groupby([colmap["campaign_id"], colmap["campaign_name"]], dropna=False)[
                [colmap["spend"], colmap["impressions"], colmap["clicks"], colmap["nbobs"]]
            ]
            .sum()
        )

        acc = grouped if acc is None else acc.add(grouped, fill_value=0)

    if acc is None:
        # Empty file beyond header
        out_path = out_dir / f"{tsv_path.stem}_campaign_rollup.xlsx"
        pd.DataFrame(
            columns=["campaignID", "campaign_name", "spend", "impressions", "clicks", "nbobs"]
        ).to_excel(out_path, index=False)
        return out_path

    # Tidy output
    out = acc.reset_index().rename(
        columns={
            colmap["campaign_id"]: "campaignID",
            colmap["campaign_name"]: "campaign_name",
            colmap["spend"]: "spend",
            colmap["impressions"]: "impressions",
            colmap["clicks"]: "clicks",
            colmap["nbobs"]: "nbobs",
        }
    )

    # Sort: biggest spend first
    out = out.sort_values(["spend", "impressions"], ascending=[False, False])

    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"{tsv_path.stem}_campaign_rollup.xlsx"
    out.to_excel(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Campaign rollup: group campaignID+name, sum spend/impressions/clicks/nbobs.")
    ap.add_argument("input", help="Path to a .tsv file OR a folder containing .tsv files")
    ap.add_argument("--chunksize", type=int, default=500_000, help="Rows per chunk (bigger = faster, more RAM).")
    ap.add_argument("--combine", action="store_true", help="Also write one combined rollup across all TSVs.")
    args = ap.parse_args()

    p = Path(args.input).expanduser().resolve()
    out_dir = (p if p.is_dir() else p.parent) / "rollups_xlsx"
    out_dir.mkdir(exist_ok=True)

    tsv_files = sorted(p.glob("*.tsv")) if p.is_dir() else [p]

    combined_acc = None
    combined_cols = None

    for tsv in tsv_files:
        print(f"Processing: {tsv.name}")
        out_xlsx = rollup_tsv(tsv, out_dir=out_dir, chunksize=args.chunksize)
        print(f"  ✅ wrote: {out_xlsx.name}")

        if args.combine:
            # Load the just-written rollup (small) and add into combined
            df = pd.read_excel(out_xlsx, engine="openpyxl")
            combined_cols = df.columns
            g = df.set_index(["campaignID", "campaign_name"])[["spend", "impressions", "clicks", "nbobs"]]
            combined_acc = g if combined_acc is None else combined_acc.add(g, fill_value=0)

    if args.combine and combined_acc is not None:
        combined_out = combined_acc.reset_index().sort_values(["spend", "impressions"], ascending=[False, False])
        combined_path = out_dir / "COMBINED_campaign_rollup.xlsx"
        combined_out.to_excel(combined_path, index=False)
        print(f"\n✅ Combined rollup written: {combined_path.name}")

    print(f"\nDone. Output folder: {out_dir}")


if __name__ == "__main__":
    main()


python campaign_rollup.py "D:\data\tsvs"
