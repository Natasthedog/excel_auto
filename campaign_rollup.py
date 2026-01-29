from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


COL_CANDIDATES = {
    "campaign_id": ["campaignid", "campaign_id", "campaign id", "campaign-id", "idcampaign", "campaignidentifier"],
    "campaign_name": ["campaignname", "campaign_name", "campaign name", "campaign-name", "namecampaign"],
    "spend": ["spend", "cost", "amount", "adspend", "media spend", "media_spend"],
    "impressions": ["impressions", "imps", "impr", "views"],
    "clicks": ["clicks", "click", "clk"],
}


def _norm(s: str) -> str:
    return "".join(ch for ch in s.strip().lower() if ch.isalnum())


def find_required_columns(columns: Iterable[str]) -> dict[str, str]:
    norm_map = {_norm(c): c for c in columns}

    found: dict[str, str] = {}
    for canonical, candidates in COL_CANDIDATES.items():
        for cand in candidates:
            key = _norm(cand)
            if key in norm_map:
                found[canonical] = norm_map[key]
                break

    missing = [k for k in ["campaign_id", "campaign_name", "spend", "impressions", "clicks"] if k not in found]
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}\n"
            f"Available columns: {list(columns)}\n"
            f"Tip: add your header aliases to COL_CANDIDATES."
        )
    return found


def detect_encoding(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8") as f:
            f.readline()
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def coerce_number(series: pd.Series, is_money: bool = False) -> pd.Series:
    s = series.astype(str).str.strip()

    if is_money:
        has_comma = s.str.contains(",", na=False)
        has_dot = s.str.contains(r"\.", na=False)
        # both -> assume comma is thousands sep
        s = s.where(~(has_comma & has_dot), s.str.replace(",", "", regex=False))
        # comma only -> assume comma is decimal
        s = s.where(~(has_comma & ~has_dot), s.str.replace(",", ".", regex=False))
        s = (
            s.str.replace("€", "", regex=False)
             .str.replace("$", "", regex=False)
             .str.replace(" ", "", regex=False)
        )

    return pd.to_numeric(s, errors="coerce").fillna(0)


def rollup_tsv(tsv_path: Path, out_dir: Path, chunksize: int = 500_000) -> Path:
    encoding = detect_encoding(tsv_path)

    header_df = pd.read_csv(tsv_path, sep="\t", nrows=0, encoding=encoding, encoding_errors="replace")
    colmap = find_required_columns(header_df.columns)

    id_col = colmap["campaign_id"]
    name_col = colmap["campaign_name"]
    spend_col = colmap["spend"]
    imp_col = colmap["impressions"]
    clk_col = colmap["clicks"]

    usecols = [id_col, name_col, spend_col, imp_col, clk_col]

    # Accumulators: sums and counts
    sums_acc = None
    cnt_acc = None

    reader = pd.read_csv(
        tsv_path,
        sep="\t",
        usecols=usecols,
        dtype={id_col: "string", name_col: "string"},
        chunksize=chunksize,
        encoding=encoding,
        encoding_errors="replace",
        low_memory=True,
    )

    for chunk in reader:
        # Clean keys
        chunk[id_col] = chunk[id_col].fillna("").astype("string")
        chunk[name_col] = chunk[name_col].fillna("").astype("string")

        # Coerce numerics
        chunk[spend_col] = coerce_number(chunk[spend_col], is_money=True)
        chunk[imp_col] = coerce_number(chunk[imp_col])
        chunk[clk_col] = coerce_number(chunk[clk_col])

        keys = [id_col, name_col]

        # Sum metrics
        sums = chunk.groupby(keys, dropna=False)[[spend_col, imp_col, clk_col]].sum()

        # nbobs = row count per group (observations)
        cnt = chunk.groupby(keys, dropna=False).size().astype("int64")

        sums_acc = sums if sums_acc is None else sums_acc.add(sums, fill_value=0)
        cnt_acc = cnt if cnt_acc is None else cnt_acc.add(cnt, fill_value=0)

    if sums_acc is None:
        out = pd.DataFrame(columns=["campaignID", "campaign_name", "spend", "impressions", "clicks", "nbobs"])
    else:
        out = sums_acc.reset_index().rename(
            columns={
                id_col: "campaignID",
                name_col: "campaign_name",
                spend_col: "spend",
                imp_col: "impressions",
                clk_col: "clicks",
            }
        )
        out["nbobs"] = cnt_acc.reset_index(drop=True).astype("int64")

        # Optional sort
        out = out.sort_values(["spend", "impressions"], ascending=[False, False])

    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"{tsv_path.stem}_campaign_rollup.xlsx"
    out.to_excel(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Campaign rollup: group campaignID+name, sum spend/impressions/clicks, nbobs=row count.")
    ap.add_argument("input", help="Path to a .tsv file OR a folder containing .tsv files")
    ap.add_argument("--chunksize", type=int, default=500_000, help="Rows per chunk (bigger = faster, more RAM).")
    ap.add_argument("--combine", action="store_true", help="Also write one combined rollup across all TSVs.")
    args = ap.parse_args()

    p = Path(args.input).expanduser().resolve()
    out_dir = (p if p.is_dir() else p.parent) / "rollups_xlsx"
    out_dir.mkdir(exist_ok=True)

    tsv_files = sorted(p.glob("*.tsv")) if p.is_dir() else [p]

    combined = None
    for tsv in tsv_files:
        print(f"Processing: {tsv.name}")
        out_xlsx = rollup_tsv(tsv, out_dir=out_dir, chunksize=args.chunksize)
        print(f"  ✅ wrote: {out_xlsx.name}")

        if args.combine:
            df = pd.read_excel(out_xlsx, engine="openpyxl")
            g = df.set_index(["campaignID", "campaign_name"])[["spend", "impressions", "clicks", "nbobs"]]
            combined = g if combined is None else combined.add(g, fill_value=0)

    if args.combine and combined is not None:
        combined_out = combined.reset_index().sort_values(["spend", "impressions"], ascending=[False, False])
        combined_path = out_dir / "COMBINED_campaign_rollup.xlsx"
        combined_out.to_excel(combined_path, index=False)
        print(f"\n✅ Combined rollup written: {combined_path.name}")

    print(f"\nDone. Output folder: {out_dir}")


if __name__ == "__main__":
    main()


python campaign_rollup.py "D:\data\tsvs"
