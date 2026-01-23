# app.py
import io
import base64
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dash import Dash, html, dcc, Input, Output, State, callback, no_update, ALL
from openpyxl import load_workbook
import numbers
from pptx import Presentation
from pptx.util import Inches
from pptx.enum.text import PP_ALIGN
from pptx.chart.data import ChartData
from pptx.enum.chart import XL_CHART_TYPE

app = Dash(__name__)
app.title = "Deck Automator (MVP)"
server = app.server
logger = logging.getLogger(__name__)
TEMPLATE_DIR = Path(__file__).resolve().parent / "assets" / "templates"
PROJECT_TEMPLATES = {
    "PnP": TEMPLATE_DIR / "PnP.pptx",
    "MMx": TEMPLATE_DIR / "MMx.pptx",
    "MMM": TEMPLATE_DIR / "MMM.pptx",
}


@dataclass(frozen=True)
class CompanyWeekMapper:
    """
    Maps between a sequential internal 'company_week' integer and YEARWK (YYYYWW),
    assuming YEARWK is an ISO week (ISO-8601).

    Provide at least one anchor mapping:
        (anchor_company_week -> anchor_yearwk)

    Optionally provide a second anchor to validate the mapping.
    """
    anchor_company_week: int
    anchor_yearwk: int
    check_company_week: int | None = None
    check_yearwk: int | None = None

    @staticmethod
    def _yearwk_to_monday(yearwk: int) -> date:
        y, w = divmod(yearwk, 100)
        if not (1 <= w <= 53):
            raise ValueError(f"Invalid YEARWK week number: {yearwk}")
        # Monday of ISO week
        return date.fromisocalendar(y, w, 1)

    @staticmethod
    def _monday_to_yearwk(d: date) -> int:
        iso_y, iso_w, _ = d.isocalendar()
        return iso_y * 100 + iso_w

    def __post_init__(self):
        # Optional consistency check with second anchor
        if (self.check_company_week is None) ^ (self.check_yearwk is None):
            raise ValueError("Provide both check_company_week and check_yearwk, or neither.")

        if self.check_company_week is not None:
            a_date = self._yearwk_to_monday(self.anchor_yearwk)
            delta = self.check_company_week - self.anchor_company_week
            derived = self._monday_to_yearwk(a_date + timedelta(weeks=delta))
            if derived != self.check_yearwk:
                raise ValueError(
                    f"Anchors inconsistent: derived {derived} but expected {self.check_yearwk}."
                )

    def to_yearwk(self, company_week: int) -> int:
        a_date = self._yearwk_to_monday(self.anchor_yearwk)
        delta_weeks = company_week - self.anchor_company_week
        out_date = a_date + timedelta(weeks=delta_weeks)
        return self._monday_to_yearwk(out_date)

    def to_company_week(self, yearwk: int) -> int:
        a_date = self._yearwk_to_monday(self.anchor_yearwk)
        target_date = self._yearwk_to_monday(yearwk)
        delta_days = (target_date - a_date).days

        if delta_days % 7 != 0:
            # Should never happen because both are Mondays, but keep it safe
            raise ValueError("Non-week-aligned difference; check inputs.")
        delta_weeks = delta_days // 7
        return self.anchor_company_week + delta_weeks

def bytes_from_contents(contents):
    _, content_string = contents.split(',')
    return base64.b64decode(content_string)


def df_from_contents(contents, filename):
    decoded = bytes_from_contents(contents)
    if filename.lower().endswith((".xlsx", ".xls", ".xlsb")):
        read_options = {}
        if filename.lower().endswith(".xlsb"):
            read_options["engine"] = "pyxlsb"
        return pd.read_excel(io.BytesIO(decoded), **read_options)
    elif filename.lower().endswith(".csv"):
        return pd.read_csv(io.StringIO(decoded.decode('utf-8')))
    else:
        raise ValueError("Unsupported file format. Please upload CSV or Excel.")


def raw_df_from_contents(contents, filename):
    decoded = bytes_from_contents(contents)
    if filename.lower().endswith((".xlsx", ".xls", ".xlsb")):
        read_options = {}
        if filename.lower().endswith(".xlsb"):
            read_options["engine"] = "pyxlsb"
        return pd.read_excel(io.BytesIO(decoded), header=None, **read_options)
    elif filename.lower().endswith(".csv"):
        return pd.read_csv(io.StringIO(decoded.decode("utf-8")), header=None)
    else:
        raise ValueError("Unsupported file format. Please upload CSV or Excel.")


def scope_df_from_contents(contents, filename):
    if not filename or not filename.lower().endswith((".xlsx", ".xlsb")):
        raise ValueError("Scope file must be an Excel workbook (.xlsx or .xlsb).")

    decoded = bytes_from_contents(contents)
    read_options = {}
    if filename.lower().endswith(".xlsb"):
        read_options["engine"] = "pyxlsb"
    scope_df = pd.read_excel(
        io.BytesIO(decoded),
        sheet_name="Overall Information",
        **read_options,
    )
    if scope_df.empty:
        return None
    return scope_df


def product_description_df_from_contents(contents, filename):
    if not filename or not filename.lower().endswith((".xlsx", ".xlsb")):
        raise ValueError("Scope file must be an Excel workbook (.xlsx or .xlsb).")

    decoded = bytes_from_contents(contents)
    read_options = {}
    if filename.lower().endswith(".xlsb"):
        read_options["engine"] = "pyxlsb"
    with pd.ExcelFile(io.BytesIO(decoded), **read_options) as excel_file:
        target_sheet = _find_sheet_by_candidates(
            excel_file.sheet_names, "PRODUCT DESCRIPTION"
        )
        if not target_sheet:
            return None
        product_df = excel_file.parse(target_sheet)
    if product_df.empty:
        return None
    return product_df


def target_brand_from_scope_df(scope_df):
    if scope_df is None or scope_df.empty:
        return None

    column_lookup = {str(col).strip().lower(): col for col in scope_df.columns}
    if "target brand" in column_lookup:
        series = scope_df[column_lookup["target brand"]].dropna()
        if not series.empty:
            return str(series.iloc[0])

    for _, row in scope_df.iterrows():
        if not len(row):
            continue
        label = str(row.iloc[0]).strip().lower()
        normalized_label = label.rstrip(":")
        if normalized_label == "target brand" and len(row) > 1 and pd.notna(row.iloc[1]):
            return str(row.iloc[1])

    return None

def modelled_category_from_scope_df(scope_df):
    if scope_df is None or scope_df.empty:
        return None

    if scope_df.shape[1] >= 2:
        for _, row in scope_df.iterrows():
            if not len(row):
                continue
            label = str(row.iloc[0]).strip()
            normalized_label = _normalize_label(label)
            if normalized_label == "category" and pd.notna(row.iloc[1]):
                return str(row.iloc[1])

    return None


def _normalize_column_name(value: str) -> str:
    return "".join(ch for ch in value.strip().lower() if ch.isalnum())


def _find_sheet_by_candidates(sheet_names: list[str], target: str) -> str | None:
    normalized_target = _normalize_column_name(target)
    normalized_sheets = {
        _normalize_column_name(str(sheet_name)): sheet_name
        for sheet_name in sheet_names
    }
    if normalized_target in normalized_sheets:
        return normalized_sheets[normalized_target]
    for normalized_sheet, sheet_name in normalized_sheets.items():
        if normalized_target in normalized_sheet or normalized_sheet in normalized_target:
            return sheet_name
    from difflib import get_close_matches

    matches = get_close_matches(
        normalized_target,
        list(normalized_sheets.keys()),
        n=1,
        cutoff=0.7,
    )
    if matches:
        return normalized_sheets[matches[0]]
    return None


def _find_column_by_candidates(df: pd.DataFrame, candidates: list[str]):
    normalized_columns = {_normalize_column_name(str(col)): col for col in df.columns}
    candidate_normalized = [_normalize_column_name(candidate) for candidate in candidates]
    for candidate in candidate_normalized:
        if candidate in normalized_columns:
            return normalized_columns[candidate]
    for column_key, column_name in normalized_columns.items():
        for candidate in candidate_normalized:
            if candidate in column_key or column_key in candidate:
                return column_name
    from difflib import get_close_matches

    matches = get_close_matches(
        " ".join(candidate_normalized),
        list(normalized_columns.keys()),
        n=1,
        cutoff=0.75,
    )
    if matches:
        return normalized_columns[matches[0]]
    return None


def _find_column_by_row_values(row: pd.Series, candidates: list[str]):
    normalized_values = {}
    for column, value in row.items():
        if pd.isna(value):
            continue
        normalized_values[_normalize_column_name(str(value))] = column
    if not normalized_values:
        return None

    candidate_normalized = [_normalize_column_name(candidate) for candidate in candidates]
    for candidate in candidate_normalized:
        if candidate in normalized_values:
            return normalized_values[candidate]
    for value_key, column_name in normalized_values.items():
        for candidate in candidate_normalized:
            if candidate in value_key or value_key in candidate:
                return column_name
    from difflib import get_close_matches

    matches = get_close_matches(
        " ".join(candidate_normalized),
        list(normalized_values.keys()),
        n=1,
        cutoff=0.75,
    )
    if matches:
        return normalized_values[matches[0]]
    return None


def _is_target_flag(value):
    if pd.isna(value):
        return False
    try:
        return float(value) == 1
    except (TypeError, ValueError):
        text = str(value).strip().lower()
        return text in {"1", "yes", "y", "true", "t"}


def _target_values_from_scope(
    scope_df: pd.DataFrame,
    target_col_candidates: list[str],
    value_col_candidates: list[str],
):
    if scope_df is None or scope_df.empty:
        return None
    target_col = _find_column_by_candidates(scope_df, target_col_candidates)
    value_col = _find_column_by_candidates(scope_df, value_col_candidates)
    if not target_col or not value_col:
        return None

    values = []
    seen = set()
    for _, row in scope_df.iterrows():
        if not _is_target_flag(row[target_col]):
            continue
        value = row[value_col]
        if pd.isna(value):
            continue
        name = str(value).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        values.append(name)
    return values or None


def target_brands_from_scope_df(scope_df: pd.DataFrame):
    return _target_values_from_scope(
        scope_df,
        ["Target Brand", "Target_Brand"],
        ["Brand", "Brand Name"],
    )


def target_manufacturers_from_scope_df(scope_df: pd.DataFrame):
    return _target_values_from_scope(
        scope_df,
        ["Target Manufacturer", "Target_Manufacturer", "Target Mfr", "Target Mfg"],
        ["Manufacturer", "Mfr", "Mfg"],
    )


def target_brands_from_product_description(product_df: pd.DataFrame):
    if product_df is None or product_df.empty:
        return None
    target_col = _find_column_by_candidates(product_df, ["Target Brand", "Target_Brand"])
    brand_col = _find_column_by_candidates(product_df, ["Brand"])
    if not target_col or not brand_col:
        return None

    brands = []
    seen = set()
    for _, row in product_df.iterrows():
        if not _is_target_flag(row[target_col]):
            continue
        brand_value = row[brand_col]
        if pd.isna(brand_value):
            continue
        brand_name = str(brand_value).strip()
        if not brand_name or brand_name in seen:
            continue
        seen.add(brand_name)
        brands.append(brand_name)
    return brands or None

def target_dimensions_from_product_description(product_df: pd.DataFrame) -> list[str]:
    if product_df is None or product_df.empty:
        return []

    normalized_columns = {
        _normalize_column_name(str(col)): col for col in product_df.columns
    }
    lines = []
    seen_dimensions = set()
    for column in product_df.columns:
        column_name = str(column)
        if not column_name.strip().lower().startswith("target"):
            continue
        base_name = column_name.strip()[len("target"):].lstrip(" _-").strip()
        if not base_name:
            continue
        base_key = _normalize_column_name(base_name)
        if base_key in seen_dimensions:
            continue
        base_column = normalized_columns.get(base_key) or _find_column_by_candidates(
            product_df, [base_name]
        )
        if not base_column:
            continue

        values = []
        seen_values = set()
        for _, row in product_df.iterrows():
            if not _is_target_flag(row[column]):
                continue
            value = row[base_column]
            if pd.isna(value):
                continue
            value_name = str(value).strip()
            if not value_name or value_name in seen_values:
                continue
            seen_values.add(value_name)
            values.append(value_name)

        if values:
            base_label = str(base_column).strip()
            lines.append(f"Target {base_label}(s): {', '.join(values)}")
            seen_dimensions.add(base_key)

    return lines

def target_lines_from_product_description(product_df: pd.DataFrame) -> list[str]:
    return target_dimensions_from_product_description(product_df)

def _find_slide_by_marker(prs, marker_text: str):
    marker_text = marker_text.strip()
    for slide in prs.slides:
        for shape in slide.shapes:
            shape_name = getattr(shape, "name", "") or ""
            if marker_text and marker_text in shape_name:
                return slide
            if shape.has_text_frame and marker_text in shape.text_frame.text:
                return slide
    return None


def _build_category_waterfall_df(gathered_df: pd.DataFrame) -> pd.DataFrame:
    vars_col = _find_column_by_candidates(
        gathered_df,
        ["Vars", "Var", "Variable", "Variable Name", "Bucket", "Driver"],
    )
    if not vars_col:
        raise ValueError("The gatheredCN10 file is missing a Vars/Variable column for the waterfall.")

    series_candidates = {
        "Base": ["Base"],
        "Promo": ["Promo", "Promotion", "Promotions"],
        "Media": ["Media"],
        "Blanks": ["Blanks", "Blank"],
        "Positives": ["Positives", "Positive", "Pos"],
        "Negatives": ["Negatives", "Negative", "Neg"],
    }
    series_columns = {}
    missing = []
    for key, candidates in series_candidates.items():
        found = _find_column_by_candidates(gathered_df, candidates)
        if not found:
            missing.append(key)
        else:
            series_columns[key] = found
    if missing:
        raise ValueError(
            "The gatheredCN10 file is missing waterfall columns: "
            + ", ".join(missing)
        )

    label_candidates = {
        "labs-Base": ["labs-Base", "labs Base", "labels-Base", "labels Base"],
        "labs-Promo": ["labs-Promo", "labs Promo", "labels-Promo", "labels Promo"],
        "labs-Media": ["labs-Media", "labs Media", "labels-Media", "labels Media"],
        "labs-Blanks": ["labs-Blanks", "labs Blanks", "labels-Blanks", "labels Blanks"],
        "labs-Positives": [
            "labs-Positives",
            "labs Positives",
            "labels-Positives",
            "labels Positives",
        ],
        "labs-Negatives": [
            "labs-Negatives",
            "labs Negatives",
            "labels-Negatives",
            "labels Negatives",
        ],
    }
    label_columns = {}
    for key, candidates in label_candidates.items():
        found = _find_column_by_candidates(gathered_df, candidates)
        if found:
            label_columns[key] = found

    ordered_cols = [vars_col] + [series_columns[key] for key in series_candidates]
    ordered_cols += list(label_columns.values())
    waterfall_df = gathered_df.loc[:, ordered_cols].copy()
    rename_map = {vars_col: "Vars", **{v: k for k, v in series_columns.items()}}
    rename_map.update({v: k for k, v in label_columns.items()})
    waterfall_df = waterfall_df.rename(columns=rename_map)

    for key in series_candidates:
        waterfall_df[key] = pd.to_numeric(waterfall_df[key], errors="coerce").fillna(0)
    if "Negatives" in waterfall_df.columns:
        waterfall_df["Negatives"] = waterfall_df["Negatives"].abs()

    return waterfall_df

def _target_level_labels_from_gathered_df(gathered_df: pd.DataFrame) -> list[str]:
    if gathered_df is None or gathered_df.empty:
        return []
    label_col = _find_column_by_candidates(
        gathered_df,
        ["Target Level Label", "Target Level", "Target Label"],
    )
    data_start_idx = 0
    if not label_col and len(gathered_df) > 0:
        header_row = gathered_df.iloc[0]
        label_col = _find_column_by_row_values(
            header_row,
            ["Target Level Label", "Target Level", "Target Label"],
        )
        if label_col:
            data_start_idx = 1
    if not label_col:
        raise ValueError("The gatheredCN10 file is missing the Target Level Label column.")
    labels = (
        gathered_df.iloc[data_start_idx:][label_col]
        .dropna()
        .astype(str)
        .map(str.strip)
    )
    unique_labels = []
    seen = set()
    for label in labels:
        if not label or label in seen:
            continue
        seen.add(label)
        unique_labels.append(label)
    return unique_labels

def _normalize_text_value(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _two_row_column_match(
    group_value: str,
    sub_value: str,
    candidates: list[str],
) -> bool:
    group_key = _normalize_column_name(group_value)
    sub_key = _normalize_column_name(sub_value)
    candidate_keys = {_normalize_column_name(candidate) for candidate in candidates}
    return group_key in candidate_keys or sub_key in candidate_keys


def _parse_two_row_header_dataframe(
    raw_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """Parse a gatheredCN10 file that uses two header rows.

    Returns the data rows with stable internal column IDs plus metadata for UI mapping.

    Example:
        >>> raw = pd.DataFrame(
        ...     [
        ...         ["Promo", "Promo", "", ""],
        ...         ["Feature", "Display", "Target Label", "Year"],
        ...         [1, 2, "Own", 2023],
        ...     ]
        ... )
        >>> data_df, meta = _parse_two_row_header_dataframe(raw)
        >>> meta["group_order"]
        ['Promo']
    """
    if raw_df is None or raw_df.empty or raw_df.shape[0] < 3:
        raise ValueError("The gatheredCN10 file must include two header rows and data rows.")
    header_row1 = raw_df.iloc[0].fillna("")
    header_row2 = raw_df.iloc[1].fillna("")
    columns_meta = []
    group_map: dict[str, list[dict]] = {}
    group_order: list[str] = []
    for idx in range(raw_df.shape[1]):
        group = str(header_row1.iloc[idx]).strip()
        subheader = str(header_row2.iloc[idx]).strip()
        col_id = f"col_{idx}"
        columns_meta.append(
            {
                "id": col_id,
                "group": group,
                "subheader": subheader,
                "position": idx,
            }
        )
        if not group:
            continue
        group_key = _normalize_column_name(group)
        if group_key in {"targetlabel", "year"}:
            continue
        if group not in group_map:
            group_map[group] = []
            group_order.append(group)
        group_map[group].append(
            {
                "id": col_id,
                "subheader": subheader,
                "position": idx,
            }
        )

    target_label_id = None
    year_id = None
    for column in columns_meta:
        if target_label_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            ["Target Label"],
        ):
            target_label_id = column["id"]
        if year_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            ["Year"],
        ):
            year_id = column["id"]

    data_df = raw_df.iloc[2:].reset_index(drop=True).copy()
    data_df.columns = [col["id"] for col in columns_meta]
    metadata = {
        "columns": columns_meta,
        "groups": group_map,
        "group_order": group_order,
        "target_label_id": target_label_id,
        "year_id": year_id,
    }
    return data_df, metadata


def _unique_column_values(data_df: pd.DataFrame, column_id: str) -> list[str]:
    if column_id not in data_df.columns:
        return []
    values = (
        data_df[column_id]
        .dropna()
        .astype(str)
        .map(str.strip)
    )
    unique_values = []
    seen = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values


def _compute_bucket_deltas(
    data_df: pd.DataFrame,
    metadata: dict,
    bucket_config: dict[str, dict[str, list[str]]],
    year1: str,
    year2: str,
) -> list[tuple[str, float]]:
    """Compute Year2-Year1 deltas for each bucket group.

    bucket_config maps group -> {"target_labels": [...], "subheaders_included": [...]}
    """
    target_label_id = metadata.get("target_label_id")
    year_id = metadata.get("year_id")
    if not target_label_id:
        raise ValueError("The gatheredCN10 file is missing the Target Label column.")
    if not year_id:
        raise ValueError("The gatheredCN10 file is missing the Year column.")

    normalized_year1 = _normalize_text_value(year1)
    normalized_year2 = _normalize_text_value(year2)

    target_series = data_df[target_label_id].map(_normalize_text_value)
    year_series = data_df[year_id].map(_normalize_text_value)

    deltas: list[tuple[str, float]] = []
    group_order = metadata.get("group_order", [])
    ordered_groups = [group for group in group_order if group in bucket_config]
    if not ordered_groups:
        ordered_groups = list(bucket_config.keys())
    for group in ordered_groups:
        config = bucket_config.get(group, {})
        selected_cols = [
            col for col in config.get("subheaders_included", []) if col in data_df.columns
        ]
        target_labels = config.get("target_labels")
        if target_labels is None:
            target_labels = ["Own", "Cross"]
        if not target_labels:
            continue
        ordered_targets = []
        normalized_targets = []
        for label in target_labels:
            normalized = _normalize_text_value(label)
            if normalized and normalized not in normalized_targets:
                normalized_targets.append(normalized)
                ordered_targets.append((label, normalized))
        target_label_sequence = []
        if "own" in normalized_targets:
            target_label_sequence.append(("Own", "own"))
        if "cross" in normalized_targets:
            target_label_sequence.append(("Cross", "cross"))
        for label, normalized in ordered_targets:
            if normalized not in {"own", "cross"}:
                target_label_sequence.append((label, normalized))
        if not target_label_sequence:
            deltas.append((group, 0.0))
            continue
        if not selected_cols:
            for label, _ in target_label_sequence:
                deltas.append((f"{label} {group}", 0.0))
            continue
        values_df = data_df[selected_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        year1_mask = year_series == normalized_year1
        year2_mask = year_series == normalized_year2
        for label, normalized in target_label_sequence:
            target_mask = target_series == normalized
            year1_sum = values_df[year1_mask & target_mask].sum().sum()
            year2_sum = values_df[year2_mask & target_mask].sum().sum()
            deltas.append((f"{label} {group}", float(year2_sum - year1_sum)))
    return deltas


def _compute_bucket_deltas_by_column(
    data_df: pd.DataFrame,
    metadata: dict,
    selected_columns: list[str],
    target_label: str,
    year1: str,
    year2: str,
) -> list[tuple[str, float]]:
    target_label_id = metadata.get("target_label_id")
    year_id = metadata.get("year_id")
    if not target_label_id:
        raise ValueError("The gatheredCN10 file is missing the Target Label column.")
    if not year_id:
        raise ValueError("The gatheredCN10 file is missing the Year column.")

    normalized_target = _normalize_text_value(target_label)
    normalized_year1 = _normalize_text_value(year1)
    normalized_year2 = _normalize_text_value(year2)

    target_series = data_df[target_label_id].map(_normalize_text_value)
    year_series = data_df[year_id].map(_normalize_text_value)
    year1_mask = (target_series == normalized_target) & (year_series == normalized_year1)
    year2_mask = (target_series == normalized_target) & (year_series == normalized_year2)

    deltas: list[tuple[str, float]] = []
    for column_id in selected_columns:
        if column_id not in data_df.columns:
            continue
        values = pd.to_numeric(data_df[column_id], errors="coerce").fillna(0)
        year1_sum = values[year1_mask].sum()
        year2_sum = values[year2_mask].sum()
        deltas.append((column_id, float(year2_sum - year1_sum)))
    return deltas


def _resolve_base_value_columns(gathered_df: pd.DataFrame) -> tuple[dict, int]:
    column_candidates = {
        "target_level": ["Target Level Label", "Target Level", "Target Label"],
        "target_label": ["Target Label", "Target", "Target Type"],
        "year": ["Year", "Model Year"],
        "actuals": ["Actuals", "Actual"],
    }
    columns = {}
    data_start_idx = 0
    header_row = gathered_df.iloc[0] if len(gathered_df) else None
    for key, candidates in column_candidates.items():
        column = _find_column_by_candidates(gathered_df, candidates)
        if not column and header_row is not None:
            column = _find_column_by_row_values(header_row, candidates)
            if column:
                data_start_idx = 1
        if not column:
            raise ValueError(
                "The gatheredCN10 file is missing the "
                f"{' / '.join(candidates)} column needed for the waterfall base."
            )
        columns[key] = column
    return columns, data_start_idx


def _waterfall_base_values(
    gathered_df: pd.DataFrame,
    target_level_label: str,
) -> tuple[float, float]:
    if gathered_df is None or gathered_df.empty:
        raise ValueError("The gatheredCN10 file is empty.")
    columns, data_start_idx = _resolve_base_value_columns(gathered_df)
    data_df = gathered_df.iloc[data_start_idx:]
    target_level = _normalize_text_value(target_level_label)
    target_level_series = data_df[columns["target_level"]].map(_normalize_text_value)
    target_label_series = data_df[columns["target_label"]].map(_normalize_text_value)
    year_series = data_df[columns["year"]].map(_normalize_text_value)
    actuals = pd.to_numeric(data_df[columns["actuals"]], errors="coerce").fillna(0)
    base_filter = (target_level_series == target_level) & (target_label_series == "own")
    year1_total = actuals[base_filter & (year_series == "year1")].sum()
    year2_total = actuals[base_filter & (year_series == "year2")].sum()
    return year1_total, year2_total


def _format_lab_base_value(value: float) -> str:
    if value is None or pd.isna(value):
        return ""
    abs_value = abs(value)
    if abs_value >= 1_000_000:
        scaled = value / 1_000_000
        suffix = "m"
    elif abs_value >= 1_000:
        scaled = value / 1_000
        suffix = "k"
    else:
        return str(int(value)) if float(value).is_integer() else str(value)
    formatted = f"{scaled:g}"
    return f"{formatted}{suffix}"


def _load_chart_workbook(chart):
    xlsx_blob = chart.part.chart_workbook.xlsx_part.blob
    return load_workbook(io.BytesIO(xlsx_blob))


def _save_chart_workbook(chart, workbook) -> None:
    stream = io.BytesIO()
    workbook.save(stream)
    chart.part.chart_workbook.xlsx_part.blob = stream.getvalue()


def _capture_label_columns(ws, series_names: list[str]) -> dict[int, dict[str, list]]:
    label_columns: dict[int, dict[str, list]] = {}
    series_lookup = {str(name).strip().lower() for name in series_names if name}
    for col_idx in range(2, ws.max_column + 1):
        header = ws.cell(row=1, column=col_idx).value
        if not header:
            continue
        header_text = str(header).strip().lower()
        if header_text in series_lookup:
            continue
        values = [
            ws.cell(row=row_idx, column=col_idx).value
            for row_idx in range(2, ws.max_row + 1)
        ]
        label_columns[col_idx] = {"header": header, "values": values}
    return label_columns


def _apply_label_columns(ws, label_columns: dict[int, dict[str, list]], total_rows: int) -> None:
    for col_idx, column in label_columns.items():
        ws.cell(row=1, column=col_idx, value=column["header"])
        values = column["values"]
        if len(values) < total_rows:
            values = values + [None] * (total_rows - len(values))
        for row_offset in range(total_rows):
            ws.cell(row=row_offset + 2, column=col_idx, value=values[row_offset])


def _update_lab_base_label(
    label_columns: dict[int, dict[str, list]],
    base_indices: tuple[int, int] | None,
    base_values: tuple[float, float] | None,
    total_rows: int,
) -> None:
    if base_indices is None or base_values is None:
        return
    formatted_values = [
        _format_lab_base_value(value) for value in base_values
    ]
    base_rows = list(base_indices)
    for column in label_columns.values():
        header = str(column["header"]).strip().lower()
        if header != "labs-base":
            continue
        values = column["values"]
        if len(values) < total_rows:
            values.extend([None] * (total_rows - len(values)))
        for idx, base_row in enumerate(base_rows):
            if base_row is None or base_row < 0:
                continue
            if base_row < len(values):
                values[base_row] = formatted_values[idx]
        column["values"] = values
        return


def _is_blank_cell(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _normalize_header_value(value: str) -> str:
    return str(value).strip().lower()


def _ensure_negatives_column_positive(ws) -> None:
    header_row = None
    header_col = None
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            value = cell.value
            if value is None:
                continue
            if _normalize_header_value(value) == "negatives":
                header_row = cell.row
                header_col = cell.column
                break
        if header_row is not None:
            break
    if header_row is None or header_col is None:
        return

    label_col = header_col - 1 if header_col > 1 else header_col
    empty_streak = 0
    for row_idx in range(header_row + 1, ws.max_row + 1):
        label_value = ws.cell(row=row_idx, column=label_col).value
        negatives_cell = ws.cell(row=row_idx, column=header_col)
        if _is_blank_cell(label_value):
            if _is_blank_cell(negatives_cell.value):
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            break
        empty_streak = 0
        value = negatives_cell.value
        if isinstance(value, numbers.Number) and not isinstance(value, bool):
            negatives_cell.value = abs(value)
            continue
        if isinstance(value, str) and value.lstrip().startswith("="):
            formula = value.lstrip()[1:].strip()
            if not (formula.lower().startswith("abs(") and formula.endswith(")")):
                negatives_cell.value = f"=ABS({formula})"


def _format_yoy_change_text(value: float) -> str:
    if value is None or pd.isna(value):
        return "0%"
    return f"{abs(value):.0%}"


def _remove_shapes_with_text(slide, targets: list[str]) -> None:
    if not targets:
        return
    for shape in list(slide.shapes):
        if not shape.has_text_frame:
            continue
        text_value = shape.text_frame.text
        if any(target in text_value for target in targets):
            element = shape._element
            element.getparent().remove(element)


def _update_waterfall_yoy_arrows(
    slide,
    base_values: tuple[float, float] | None,
) -> None:
    if base_values is None:
        return
    year1_total, year2_total = base_values
    if year1_total is None or year2_total is None:
        return
    if year1_total == 0:
        pct_change = 0.0
    else:
        pct_change = (year2_total - year1_total) / year1_total
    direction = "increase" if year2_total >= year1_total else "decrease"
    remove_placeholder = "<% decrease>" if direction == "increase" else "<% increase>"
    keep_placeholder = "<% increase>" if direction == "increase" else "<% decrease>"
    _remove_shapes_with_text(slide, [remove_placeholder])
    replacement_text = f"{_format_yoy_change_text(pct_change)} {direction}"
    replace_text_in_slide_preserve_formatting(slide, keep_placeholder, replacement_text)


def _set_waterfall_chart_title(chart, label: str | None) -> None:
    if not label:
        return
    title_text = f"{label} Waterfall"
    try:
        chart.has_title = True
        chart.chart_title.text_frame.text = title_text
    except Exception:
        return

def update_or_add_column_chart(slide, chart_name, categories, series_dict):
    """
    If a chart with name=chart_name exists on the slide, update its data.
    Otherwise insert a new clustered column chart in a sensible spot.
    Charts produced here remain EDITABLE in PowerPoint.
    """
    chart_shape = None
    for shape in slide.shapes:
        if getattr(shape, "name", None) == chart_name:
            if shape.has_chart:
                chart_shape = shape
                break
            else:
                # Remove placeholder artifacts that aren't real charts
                sp = shape._element
                sp.getparent().remove(sp)

    cd = ChartData()
    cd.categories = categories
    for s_name, values in series_dict.items():
        cd.add_series(s_name, list(values))

    if chart_shape:
        # Replace data in existing chart (preserves template styling)
        chart_shape.chart.replace_data(cd)
        return chart_shape
    else:
        # Fallback: repurpose the first chart on the slide if present.
        for shape in slide.shapes:
            if shape.has_chart:
                shape.chart.replace_data(cd)
                shape.name = chart_name
                return shape
        # Remove any stale shapes with the target name before adding a new chart
        for shape in list(slide.shapes):
            if getattr(shape, "name", None) == chart_name:
                sp = shape._element
                sp.getparent().remove(sp)

        # Insert a new chart (fallback)
        left, top, width, height = Inches(1), Inches(2), Inches(8), Inches(4.5)
        chart_shape = slide.shapes.add_chart(
            XL_CHART_TYPE.COLUMN_CLUSTERED, left, top, width, height, cd
        )
        chart_shape.name = chart_name
        chart = chart_shape.chart
        # Light touch formatting; rely on template/theme for styling
        chart.has_legend = True
        return chart

def update_or_add_waterfall_chart(slide, chart_name, categories, series_dict):
    """
    Update an existing waterfall chart or insert one if missing.
    """
    chart_shape = None
    for shape in slide.shapes:
        if getattr(shape, "name", None) == chart_name and shape.has_chart:
            chart_shape = shape
            break

    if chart_shape is None:
        for shape in slide.shapes:
            if shape.has_chart:
                chart_shape = shape
                shape.name = chart_name
                break

    cd = ChartData()
    cd.categories = categories
    for s_name, values in series_dict.items():
        cd.add_series(s_name, list(values))

    if chart_shape:
        chart_shape.chart.replace_data(cd)
        return chart_shape

    waterfall_type = getattr(XL_CHART_TYPE, "WATERFALL", XL_CHART_TYPE.COLUMN_STACKED)
    left, top, width, height = Inches(1), Inches(2), Inches(8), Inches(4.5)
    chart_shape = slide.shapes.add_chart(
        waterfall_type, left, top, width, height, cd
    )
    chart_shape.name = chart_name
    return chart_shape

def set_text_by_name(slide, shape_name, text):
    for shape in slide.shapes:
        if getattr(shape, "name", None) == shape_name and shape.has_text_frame:
            tf = shape.text_frame
            tf.clear()
            p = tf.paragraphs[0]
            run = p.add_run()
            run.text = str(text)
            p.alignment = PP_ALIGN.LEFT
            return True
    return False


def replace_text_in_slide(slide, old_text, new_text):
    replaced = False
    for shape in slide.shapes:
        if not shape.has_text_frame:
            continue
        text_frame = shape.text_frame
        current_text = text_frame.text
        if old_text in current_text:
            text_frame.text = current_text.replace(old_text, new_text)
            replaced = True
    return replaced


def replace_text_in_slide_preserve_formatting(slide, old_text, new_text):
    replaced = False
    for shape in slide.shapes:
        if not shape.has_text_frame:
            continue
        for paragraph in shape.text_frame.paragraphs:
            for run in paragraph.runs:
                if old_text in run.text:
                    run.text = run.text.replace(old_text, new_text)
                    replaced = True
    return replaced


def append_text_after_label(slide, label_text, appended_text):
    if not appended_text:
        return False
    for shape in slide.shapes:
        if not shape.has_text_frame:
            continue
        for paragraph in shape.text_frame.paragraphs:
            if label_text not in paragraph.text:
                continue
            if appended_text in paragraph.text:
                return True
            spacer = "" if label_text.endswith(" ") else " "
            for run in paragraph.runs:
                if label_text in run.text:
                    new_run = paragraph.add_run()
                    new_run.text = f"{spacer}{appended_text}"
                    return True
            new_run = paragraph.add_run()
            new_run.text = f"{spacer}{appended_text}"
            return True
    return False


def append_paragraph_after_label(slide, label_text, appended_text):
    if not appended_text:
        return False
    for shape in slide.shapes:
        if not shape.has_text_frame:
            continue
        text_frame = shape.text_frame
        if label_text not in text_frame.text:
            continue
        if any(paragraph.text.strip() == appended_text for paragraph in text_frame.paragraphs):
            return True
        for paragraph in text_frame.paragraphs:
            if label_text in paragraph.text:
                new_paragraph = text_frame.add_paragraph()
                new_paragraph.text = appended_text
                paragraph._p.addnext(new_paragraph._p)
                return True
    return False


def append_paragraphs_after_label(slide, label_text, appended_texts):
    if not appended_texts:
        return False
    appended_texts = [text for text in appended_texts if text and text.strip()]
    if not appended_texts:
        return False
    for shape in slide.shapes:
        if not shape.has_text_frame:
            continue
        text_frame = shape.text_frame
        if label_text not in text_frame.text:
            continue
        existing_texts = {paragraph.text.strip() for paragraph in text_frame.paragraphs}
        to_add = [text for text in appended_texts if text not in existing_texts]
        if not to_add:
            return True
        insert_after = None
        for paragraph in text_frame.paragraphs:
            if label_text in paragraph.text:
                insert_after = paragraph
                break
        if insert_after is None:
            continue
        last_paragraph = insert_after
        for text in to_add:
            new_paragraph = text_frame.add_paragraph()
            new_paragraph.text = text
            last_paragraph._p.addnext(new_paragraph._p)
            last_paragraph = new_paragraph
        return True
    return False

def add_table(slide, table_name, df: pd.DataFrame):
    # Identify an existing table to reuse, preferring one with the expected name.
    target_shape = None
    for shape in slide.shapes:
        if getattr(shape, "name", None) == table_name and shape.has_table:
            target_shape = shape
            break

    if target_shape is None:
        for shape in slide.shapes:
            if shape.has_table:
                target_shape = shape
                target_shape.name = table_name
                break

    if target_shape and target_shape.has_table:
        tbl = target_shape.table
        # Resize (simple): write headers to row 0, then rows afterward if room allows
        n_rows = min(len(df) + 1, tbl.rows.__len__())
        n_cols = min(len(df.columns), tbl.columns.__len__())
        # headers
        for j, col in enumerate(df.columns[:n_cols]):
            cell = tbl.cell(0, j)
            cell.text = str(col)
        # cells
        for i in range(1, n_rows):
            for j in range(n_cols):
                tbl.cell(i, j).text = str(df.iloc[i-1, j])
        # Clear any leftover rows beyond the populated range
        for i in range(n_rows, tbl.rows.__len__()):
            for j in range(tbl.columns.__len__()):
                tbl.cell(i, j).text = ""
        return True

    # Remove non-table placeholders with the desired name so we can insert a fresh table.
    for shape in list(slide.shapes):
        if getattr(shape, "name", None) == table_name and not shape.has_table:
            sp = shape._element
            sp.getparent().remove(sp)

    # Otherwise, add a new table
    rows, cols = len(df) + 1, len(df.columns)
    left, top, width, height = Inches(1), Inches(1.5), Inches(8), Inches(1 + 0.3 * rows)
    table_shape = slide.shapes.add_table(rows, cols, left, top, width, height)
    table_shape.name = table_name
    table = table_shape.table
    for j, col in enumerate(df.columns):
        table.cell(0, j).text = str(col)
    for i in range(len(df)):
        for j in range(len(df.columns)):
            table.cell(i+1, j).text = str(df.iloc[i, j])

    # Avoid manipulating the low-level XML that may not exist across templates.
    # python-pptx represents the table as a ``CT_GraphicalObjectFrame`` whose
    # schema does not expose a ``graphicFrame`` attribute.  Some versions of the
    # library can therefore raise an AttributeError when we try to clear borders
    # by touching ``graphicFrame`` directly.  Since this styling tweak is only a
    # nice-to-have, we simply rely on the template/theme defaults instead of
    # editing the XML manually.
    return True

def remove_empty_placeholders(slide):
    """Remove placeholder shapes that have no meaningful content."""
    for shape in list(slide.shapes):
        if not getattr(shape, "is_placeholder", False):
            continue

        # Keep placeholders that now contain text, tables, or charts with data.
        if shape.has_text_frame:
            if shape.text_frame.text and shape.text_frame.text.strip():
                continue
        elif shape.has_table:
            # If every cell is blank, treat as empty.
            if any(
                cell.text.strip()
                for row in shape.table.rows
                for cell in row.cells
            ):
                continue
        elif shape.has_chart:
            # Assume populated charts should remain.
            continue

        sp = shape._element
        sp.getparent().remove(sp)


def _normalize_label(value: str) -> str:
    return " ".join(value.strip().lower().replace(":", "").split())


def _find_company_week_value(scope_df: pd.DataFrame, label: str):
    if scope_df is None or scope_df.empty or scope_df.shape[1] < 2:
        raise ValueError("Scope file must include labels in column A and company weeks in column B.")
    label_normalized = _normalize_label(label)
    candidates = []
    for _, row in scope_df.iterrows():
        cell_value = row.iloc[0]
        if pd.isna(cell_value):
            continue
        cell_label = _normalize_label(str(cell_value))
        candidates.append((cell_label, row))
        if label_normalized in cell_label or cell_label in label_normalized:
            value = row.iloc[1]
            if pd.isna(value):
                raise ValueError(f"Missing company week value for '{label}'.")
            return value
    if candidates:
        from difflib import get_close_matches

        labels = [candidate_label for candidate_label, _ in candidates]
        matches = get_close_matches(label_normalized, labels, n=1, cutoff=0.7)
        if matches:
            matched_label = matches[0]
            for candidate_label, row in candidates:
                if candidate_label == matched_label:
                    value = row.iloc[1]
                    if pd.isna(value):
                        raise ValueError(f"Missing company week value for '{label}'.")
                    return value
    raise ValueError(f"Could not find '{label}' in the scope file.")


def _coerce_yearwk(value) -> int:
    if pd.isna(value):
        raise ValueError("Missing company week value for modelling period.")
    raw = str(value).strip()
    if not raw:
        raise ValueError("Missing company week value for modelling period.")
    try:
        numeric_value = int(float(raw))
    except ValueError:
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            raise ValueError("Company week value must be a YYYYWW-style week number or a company week.")
        numeric_value = int(digits)

    if len(str(numeric_value)) <= 4:
        mapper = CompanyWeekMapper(
            anchor_company_week=2455,
            anchor_yearwk=202638,
            check_company_week=2470,
            check_yearwk=202653,
        )
        return mapper.to_yearwk(numeric_value)

    year, week = divmod(numeric_value, 100)
    if year <= 0 or not (1 <= week <= 53):
        raise ValueError("Company week value must be a YYYYWW-style week number or a company week.")
    return numeric_value


def _format_modelling_period(data_df: pd.DataFrame, scope_df: pd.DataFrame) -> tuple[str, int]:
    start_company_week = _find_company_week_value(scope_df, "First week of modelling")
    end_company_week = _find_company_week_value(scope_df, "Last week of modelling")
    start_yearwk = _coerce_yearwk(start_company_week)
    end_yearwk = _coerce_yearwk(end_company_week)
    start_date = CompanyWeekMapper._yearwk_to_monday(start_yearwk)
    end_date = CompanyWeekMapper._yearwk_to_monday(end_yearwk) + timedelta(days=6)
    week_count = ((end_date - start_date).days // 7) + 1
    return f"{start_date:%b %d, %Y} - {end_date:%b %d, %Y}", week_count


def _format_study_year_range(scope_df: pd.DataFrame) -> str:
    start_company_week = _find_company_week_value(scope_df, "First week of modelling")
    end_company_week = _find_company_week_value(scope_df, "Last week of modelling")
    start_yearwk = _coerce_yearwk(start_company_week)
    end_yearwk = _coerce_yearwk(end_company_week)
    start_year = CompanyWeekMapper._yearwk_to_monday(start_yearwk).year
    end_year = CompanyWeekMapper._yearwk_to_monday(end_yearwk).year
    if start_year == end_year:
        return str(start_year)
    return f"{start_year}-{end_year}"


def set_time_period_text(slide, label_text, time_period, week_count):
    for shape in slide.shapes:
        if not shape.has_text_frame:
            continue
        text_frame = shape.text_frame
        if label_text not in text_frame.text:
            continue
        label_index = None
        for idx, paragraph in enumerate(text_frame.paragraphs):
            if label_text in paragraph.text:
                label_index = idx
                break
        if label_index is None:
            continue
        paragraphs = list(text_frame.paragraphs)
        if label_index + 1 < len(paragraphs):
            value_paragraph = paragraphs[label_index + 1]
        else:
            value_paragraph = text_frame.add_paragraph()
        value_paragraph.text = f"{time_period} (number of weeks = {week_count})"
        for extra_paragraph in paragraphs[label_index + 2:]:
            extra_paragraph.text = ""
        return True
    return False


def _modelling_period_bounds(scope_df: pd.DataFrame) -> tuple[date, date]:
    start_company_week = _find_company_week_value(scope_df, "First week of modelling")
    end_company_week = _find_company_week_value(scope_df, "Last week of modelling")
    start_yearwk = _coerce_yearwk(start_company_week)
    end_yearwk = _coerce_yearwk(end_company_week)
    earliest_yearwk = min(start_yearwk, end_yearwk)
    latest_yearwk = max(start_yearwk, end_yearwk)
    earliest_date = CompanyWeekMapper._yearwk_to_monday(earliest_yearwk)
    latest_date = CompanyWeekMapper._yearwk_to_monday(latest_yearwk) + timedelta(days=6)
    return earliest_date, latest_date


def _replace_modelling_period_placeholders_in_categories(
    categories: list[str],
    scope_df: pd.DataFrame | None,
) -> list[str]:
    if scope_df is None or not categories:
        return categories
    try:
        start_date, end_date = _modelling_period_bounds(scope_df)
    except Exception:
        return categories
    earliest = start_date.strftime("%b %d, %Y")
    latest = end_date.strftime("%b %d, %Y")
    updated = []
    for value in categories:
        text = "" if value is None else str(value)
        if "<earliest date>" in text or "<latest date>" in text:
            text = text.replace("<earliest date>", earliest)
            text = text.replace("<latest date>", latest)
        updated.append(text)
    return updated


def _waterfall_chart_from_slide(slide, chart_name: str):
    for shape in slide.shapes:
        if shape.has_chart and chart_name in (getattr(shape, "name", "") or ""):
            return shape.chart
    for shape in slide.shapes:
        if shape.has_chart:
            return shape.chart
    return None


def _waterfall_chart_shape_from_slide(slide, chart_name: str):
    for shape in slide.shapes:
        if shape.has_chart and chart_name in (getattr(shape, "name", "") or ""):
            return shape
    for shape in slide.shapes:
        if shape.has_chart:
            return shape
    return None


def _categories_from_chart(chart) -> list[str]:
    categories = []
    try:
        plot_categories = chart.plots[0].categories
    except Exception:
        plot_categories = []
    for category in plot_categories:
        label = getattr(category, "label", None)
        categories.append(str(label) if label is not None else str(category))
    return categories


def _waterfall_base_indices(categories: list[str]) -> tuple[int, int] | None:
    earliest_idx = None
    latest_idx = None
    for idx, value in enumerate(categories):
        text = "" if value is None else str(value)
        if "<earliest date>" in text:
            earliest_idx = idx
        if "<latest date>" in text:
            latest_idx = idx
    if earliest_idx is None or latest_idx is None:
        matches = [
            idx
            for idx, value in enumerate(categories)
            if "52 w/e" in ("" if value is None else str(value)).lower()
        ]
        if len(matches) >= 2:
            earliest_idx = matches[0] if earliest_idx is None else earliest_idx
            latest_idx = matches[-1] if latest_idx is None else latest_idx
    if earliest_idx is None or latest_idx is None:
        return None
    return earliest_idx, latest_idx


def _apply_bucket_categories(
    categories: list[str],
    bucket_labels: list[str],
    base_indices: tuple[int, int],
) -> tuple[list[str], tuple[int, int]]:
    if not bucket_labels:
        return categories, base_indices
    start_idx, end_idx = base_indices
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx
    prefix = categories[: start_idx + 1]
    suffix = categories[end_idx:]
    new_categories = prefix + bucket_labels + suffix
    new_end_idx = start_idx + len(bucket_labels) + 1
    return new_categories, (start_idx, new_end_idx)


def _apply_bucket_values(
    values: list[float],
    base_indices: tuple[int, int],
    bucket_values: list[float],
) -> list[float]:
    if not bucket_values:
        return values
    start_idx, end_idx = base_indices
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx
    if end_idx >= len(values):
        values = values + [0.0] * (end_idx - len(values) + 1)
    prefix = values[: start_idx + 1]
    suffix = values[end_idx:]
    return prefix + bucket_values + suffix


def _apply_bucket_placeholders(
    values: list[float],
    base_indices: tuple[int, int],
    bucket_count: int,
    fill_value: float = 0.0,
) -> list[float]:
    if bucket_count <= 0:
        return values
    fill_values = [fill_value] * bucket_count
    return _apply_bucket_values(values, base_indices, fill_values)


def _should_update_base_series(chart_series) -> bool:
    name = getattr(chart_series, "name", "")
    if not name:
        return False
    return "base" in str(name).lower()


def _is_blanks_series(chart_series) -> bool:
    name = getattr(chart_series, "name", "")
    return "blank" in str(name).lower()


def _is_positive_series(chart_series) -> bool:
    name = getattr(chart_series, "name", "")
    return "positive" in str(name).lower()


def _is_negative_series(chart_series) -> bool:
    name = getattr(chart_series, "name", "")
    return "negative" in str(name).lower()


def _bucket_value_split(bucket_values: list[float]) -> tuple[list[float], list[float]]:
    positives: list[float] = []
    negatives: list[float] = []
    for value in bucket_values:
        if value >= 0:
            positives.append(value)
            negatives.append(0.0)
        else:
            positives.append(0.0)
            negatives.append(value)
    return positives, negatives


def _bucket_blank_values(bucket_values: list[float], base_value: float) -> list[float]:
    blanks: list[float] = []
    running_total = base_value
    for value in bucket_values:
        blanks.append(running_total)
        running_total += value
    return blanks


def _build_waterfall_chart_data(
    chart,
    scope_df: pd.DataFrame | None,
    gathered_df: pd.DataFrame | None = None,
    target_level_label: str | None = None,
    bucket_labels: list[str] | None = None,
    bucket_values: list[float] | None = None,
) -> tuple[
    ChartData,
    list[str],
    tuple[int, int] | None,
    tuple[float, float] | None,
    list[tuple[str, list[float]]],
]:
    categories = _categories_from_chart(chart)
    base_indices = _waterfall_base_indices(categories)
    categories = _replace_modelling_period_placeholders_in_categories(categories, scope_df)
    original_base_indices = base_indices
    bucket_labels = list(bucket_labels or [])
    bucket_values = [float(value) for value in (bucket_values or [])]
    if bucket_labels and bucket_values:
        bucket_len = min(len(bucket_labels), len(bucket_values))
        bucket_labels = bucket_labels[:bucket_len]
        bucket_values = bucket_values[:bucket_len]
    if bucket_labels and base_indices:
        categories, base_indices = _apply_bucket_categories(
            categories,
            bucket_labels,
            base_indices,
        )
    bucket_count = len(bucket_labels)
    base_values = None
    if (
        gathered_df is not None
        and target_level_label
        and base_indices is not None
    ):
        base_values = _waterfall_base_values(gathered_df, target_level_label)
    cd = ChartData()
    cd.categories = categories
    base_start_value = None
    if base_values and base_values[0] is not None:
        base_start_value = float(base_values[0])
    elif base_indices is not None:
        for series in chart.series:
            if _should_update_base_series(series):
                series_values = list(series.values)
                if base_indices[0] < len(series_values):
                    base_start_value = float(series_values[base_indices[0]])
                break
    if base_start_value is None:
        base_start_value = 0.0

    positive_bucket_values = []
    negative_bucket_values = []
    blank_bucket_values = []
    if bucket_labels and bucket_values:
        positive_bucket_values, negative_bucket_values = _bucket_value_split(bucket_values)
        blank_bucket_values = _bucket_blank_values(bucket_values, base_start_value)

    series_values: list[tuple[str, list[float]]] = []
    for series in chart.series:
        values = list(series.values)
        if original_base_indices and bucket_labels:
            if _is_positive_series(series):
                if positive_bucket_values:
                    values = _apply_bucket_values(
                        values,
                        original_base_indices,
                        positive_bucket_values,
                    )
                else:
                    values = _apply_bucket_placeholders(
                        values,
                        original_base_indices,
                        bucket_count,
                    )
            elif _is_negative_series(series):
                if negative_bucket_values:
                    values = _apply_bucket_values(
                        values,
                        original_base_indices,
                        negative_bucket_values,
                    )
                else:
                    values = _apply_bucket_placeholders(
                        values,
                        original_base_indices,
                        bucket_count,
                    )
            elif _is_blanks_series(series):
                if blank_bucket_values:
                    values = _apply_bucket_values(
                        values,
                        original_base_indices,
                        blank_bucket_values,
                    )
                else:
                    values = _apply_bucket_placeholders(
                        values,
                        original_base_indices,
                        bucket_count,
                    )
            else:
                values = _apply_bucket_placeholders(
                    values,
                    original_base_indices,
                    bucket_count,
                )
        if base_values and base_indices:
            should_update = _should_update_base_series(series)
            if not should_update and len(chart.series) == 1:
                should_update = True
            if should_update:
                if base_indices[0] < len(values):
                    values[base_indices[0]] = base_values[0]
                if base_indices[1] < len(values):
                    values[base_indices[1]] = base_values[1]
        cd.add_series(series.name, values)
        series_values.append((series.name, values))
    return cd, categories, base_indices, base_values, series_values


def _add_waterfall_chart_from_template(
    slide,
    template_slide,
    scope_df: pd.DataFrame | None,
    gathered_df: pd.DataFrame | None,
    target_level_label: str | None,
    bucket_data: dict | None,
    chart_name: str,
):
    template_shape = _waterfall_chart_shape_from_slide(template_slide, chart_name)
    if template_shape is None:
        raise ValueError("Could not find the waterfall chart on the <Waterfall Template> slide.")
    template_chart = template_shape.chart
    template_series_names = [series.name for series in template_chart.series]
    label_columns = _capture_label_columns(
        _load_chart_workbook(template_chart).active,
        template_series_names,
    )
    cd, categories, base_indices, base_values, _ = _build_waterfall_chart_data(
        template_chart,
        scope_df,
        gathered_df,
        target_level_label,
        bucket_data.get("labels") if bucket_data else None,
        bucket_data.get("values") if bucket_data else None,
    )
    chart_type = getattr(
        template_chart,
        "chart_type",
        getattr(XL_CHART_TYPE, "WATERFALL", XL_CHART_TYPE.COLUMN_STACKED),
    )
    chart_shape = slide.shapes.add_chart(
        chart_type,
        template_shape.left,
        template_shape.top,
        template_shape.width,
        template_shape.height,
        cd,
    )
    chart_shape.name = getattr(template_shape, "name", chart_name)
    updated_wb = _load_chart_workbook(chart_shape.chart)
    total_rows = len(categories)
    _update_lab_base_label(
        label_columns,
        base_indices,
        base_values,
        total_rows,
    )
    _apply_label_columns(updated_wb.active, label_columns, total_rows)
    _ensure_negatives_column_positive(updated_wb.active)
    _save_chart_workbook(chart_shape.chart, updated_wb)
    _update_waterfall_yoy_arrows(slide, base_values)
    return chart_shape


def _set_waterfall_slide_header(slide, label: str) -> None:
    title_text = label
    replaced = replace_text_in_slide_preserve_formatting(
        slide, "<Waterfall Template>", title_text
    )
    replaced = (
        replace_text_in_slide_preserve_formatting(slide, "Waterfall Template", title_text)
        or replaced
    )
    if replaced:
        return
    for shape in slide.shapes:
        if shape.has_text_frame:
            shape.text_frame.text = title_text
            return


def _update_waterfall_chart(
    slide,
    scope_df: pd.DataFrame | None,
    gathered_df: pd.DataFrame | None,
    target_level_label: str | None,
    bucket_data: dict | None,
) -> None:
    chart = _waterfall_chart_from_slide(slide, "Waterfall Template")
    if chart is None:
        raise ValueError("Could not find the waterfall chart on the <Waterfall Template> slide.")
    series_names = [series.name for series in chart.series]
    label_columns = _capture_label_columns(_load_chart_workbook(chart).active, series_names)
    cd, categories, base_indices, base_values, _ = _build_waterfall_chart_data(
        chart,
        scope_df,
        gathered_df,
        target_level_label,
        bucket_data.get("labels") if bucket_data else None,
        bucket_data.get("values") if bucket_data else None,
    )
    chart.replace_data(cd)
    updated_wb = _load_chart_workbook(chart)
    total_rows = len(categories)
    _update_lab_base_label(
        label_columns,
        base_indices,
        base_values,
        total_rows,
    )
    _apply_label_columns(updated_wb.active, label_columns, total_rows)
    _ensure_negatives_column_positive(updated_wb.active)
    _save_chart_workbook(chart, updated_wb)
    _update_waterfall_yoy_arrows(slide, base_values)


def populate_category_waterfall(
    prs,
    gathered_df: pd.DataFrame,
    scope_df: pd.DataFrame | None = None,
    target_labels: list[str] | None = None,
    bucket_data: dict | None = None,
):
    template_slide = _find_slide_by_marker(prs, "<Waterfall Template>")
    if template_slide is None:
        raise ValueError("Could not find the <Waterfall Template> slide in the template.")
    labels = target_labels or _target_level_labels_from_gathered_df(gathered_df)
    if not labels:
        return
    first_slide = template_slide
    _update_waterfall_chart(first_slide, scope_df, gathered_df, labels[0], bucket_data)
    _set_waterfall_slide_header(first_slide, labels[0])
    for label in labels[1:]:
        new_slide = prs.slides.add_slide(template_slide.slide_layout)
        _add_waterfall_chart_from_template(
            new_slide,
            template_slide,
            scope_df,
            gathered_df,
            label,
            bucket_data,
            "Waterfall Template",
        )
        _set_waterfall_slide_header(new_slide, label)

def build_pptx_from_template(
    template_bytes,
    df,
    target_brand=None,
    project_name=None,
    scope_df=None,
    product_description_df=None,
    waterfall_targets=None,
    bucket_data=None,
):
    prs = Presentation(io.BytesIO(template_bytes))
    # Assume Slide 1 has TitleBox & SubTitle
    slide1 = prs.slides[0]
    set_text_by_name(slide1, "TitleBox", "Monthly Performance Summary")
    set_text_by_name(slide1, "SubTitle", "Auto-generated via Dash + python-pptx")
    if target_brand:
        replace_text_in_slide(slide1, "Target Brand", target_brand)
    if project_name == "MMx" and scope_df is not None:
        try:
            year_range = _format_study_year_range(scope_df)
        except Exception:
            year_range = None
        if year_range:
            replace_text_in_slide_preserve_formatting(slide1, "<RANGE>", year_range)
        generation_date = date.today().strftime("%b %d, %Y")
        replace_text_in_slide_preserve_formatting(slide1, "<DATE>", generation_date)
    remove_empty_placeholders(slide1)

    # Assume Slide 2 is for a KPI table and a chart
    slide2 = prs.slides[1] if len(prs.slides) > 1 else prs.slides.add_slide(prs.slide_layouts[5])

    # Simple KPIs (example): top 5 brands by value
    if "Brand" in df.columns and "Value" in df.columns:
        kpis = (
            df.groupby("Brand", as_index=False)["Value"].sum()
              .sort_values("Value", ascending=False)
              .head(5)
        )
        add_table(slide2, "Table_Summary", kpis)

        # Chart: share by Brand (editable)
        categories = kpis["Brand"].tolist()
        series = {"Value": kpis["Value"].tolist()}
        update_or_add_column_chart(slide2, "Chart_ShareByBrand", categories, series)

    remove_empty_placeholders(slide2)

    if project_name == "MMx" and len(prs.slides) > 3:
        slide4 = prs.slides[3]
        if scope_df is not None:
            try:
                modelled_category = modelled_category_from_scope_df(scope_df)
            except Exception:
                modelled_category = None
            if modelled_category:
                append_text_after_label(slide4, "Modelled Category:", modelled_category)
        if product_description_df is not None:
            try:
                target_dimensions = target_dimensions_from_product_description(
                    product_description_df
                )
            except Exception:
                target_dimensions = []
            if target_dimensions:
                append_paragraphs_after_label(
                    slide4,
                    "Modelled Category:",
                    target_dimensions,
                )
        if scope_df is not None:
            try:
                time_period, week_count = _format_modelling_period(df, scope_df)
            except Exception:
                time_period = None
                week_count = None
            if time_period and week_count is not None:
                set_time_period_text(slide4, "TIME PERIOD", time_period, week_count)
        remove_empty_placeholders(slide4)

    if project_name == "MMx":
        try:
            populate_category_waterfall(
                prs,
                df,
                scope_df,
                waterfall_targets,
                bucket_data,
            )
        except Exception:
            logger.exception("Failed to populate category waterfall slides.")
            raise

    # Return bytes
    out = io.BytesIO()
    prs.save(out)
    out.seek(0)
    return out.read()

def render_upload_status(filename, success_label):
    if not filename:
        return html.Div("No file uploaded yet.", style={"color": "#888", "fontSize": "0.9rem"})

    return html.Div(
        [
            html.Div(
                style={
                    "height": "10px",
                    "backgroundColor": "#E5E7EB",
                    "borderRadius": "999px",
                    "overflow": "hidden",
                    "marginTop": "8px",
                },
                children=[
                    html.Div(
                        style={
                            "width": "100%",
                            "height": "100%",
                            "backgroundColor": "#22C55E",
                            "transition": "width 0.3s ease",
                        }
                    )
                ],
            ),
            html.Div(
                f"{success_label}: {filename}",
                style={"color": "#15803D", "fontSize": "0.9rem", "marginTop": "6px"},
            ),
        ]
    )

app.layout = html.Div(
    style={"maxWidth":"900px","margin":"40px auto","fontFamily":"Inter, system-ui"},
    children=[
        html.H2("PowerPoint Deck Automator (Dash + python-pptx)"),
        html.P("Upload your data, pick the project, and we will fill the matching PPTX template."),
        dcc.Store(id="bucket-metadata"),
        dcc.Store(id="bucket-config"),
        dcc.Store(id="bucket-deltas"),
        html.Div(
            [
                html.Label("Which project are you working on?"),
                dcc.Dropdown(
                    id="project-select",
                    options=[{"label": key, "value": key} for key in PROJECT_TEMPLATES],
                    placeholder="Select a project",
                    clearable=False,
                ),
            ],
            style={"marginBottom": "18px"},
        ),
        html.Div([
            html.Label("Upload gatheredCN10 file (.xlsx, .xlsb, or .csv):"),
            dcc.Upload(
                id="data-upload",
                children=html.Div(["Drag & Drop or ", html.A("Select File")]),
                accept=".xlsx,.xls,.xlsb,.csv",
                multiple=False,
                style={
                    "padding":"20px",
                    "border":"1px dashed #888",
                    "borderRadius":"12px",
                    "marginBottom":"6px",
                },
            ),
            html.Div(
                id="data-upload-status",
                children=render_upload_status(None, "gatheredCN10 upload complete"),
                style={"marginBottom":"12px"},
            ),
        ], style={"marginBottom":"18px"}),
        html.Div([
            html.Label("Upload scope file (.xlsx or .xlsb):"),
            dcc.Upload(
                id="scope-upload",
                children=html.Div(["Drag & Drop or ", html.A("Select File")]),
                accept=".xlsx,.xlsb",
                multiple=False,
                style={
                    "padding":"20px",
                    "border":"1px dashed #888",
                    "borderRadius":"12px",
                    "marginBottom":"6px",
                },
            ),
            html.Div(
                id="scope-upload-status",
                children=render_upload_status(None, "Scope upload complete"),
                style={"marginBottom":"12px"},
            ),
        ], style={"marginBottom":"18px"}),
        html.Div(
            [
                html.Label(
                    "Which Target Level Label values should be included in the waterfalls?"
                ),
                dcc.Checklist(
                    id="waterfall-targets",
                    options=[],
                    value=[],
                    labelStyle={"display": "block", "marginBottom": "6px"},
                    inputStyle={"marginRight": "8px"},
                ),
                html.Div(
                    id="waterfall-targets-status",
                    style={"color": "#6B7280", "fontSize": "0.9rem", "marginTop": "6px"},
                ),
            ],
            style={"marginBottom": "18px"},
        ),
        html.Div(
            [
                html.H3("Bucketed Waterfall Inputs"),
                html.Div(
                    [
                        html.Label("Year 1"),
                        dcc.Dropdown(
                            id="bucket-year1",
                            options=[],
                            placeholder="Select Year 1",
                            clearable=False,
                        ),
                    ],
                    style={"marginBottom": "12px"},
                ),
                html.Div(
                    [
                        html.Label("Year 2"),
                        dcc.Dropdown(
                            id="bucket-year2",
                            options=[],
                            placeholder="Select Year 2",
                            clearable=False,
                        ),
                    ],
                    style={"marginBottom": "12px"},
                ),
                html.Div(id="bucket-group-controls"),
                html.Button(
                    "Apply Buckets to Waterfall",
                    id="apply-buckets",
                    n_clicks=0,
                    style={"padding": "10px 16px", "borderRadius": "10px"},
                ),
                html.Div(
                    id="bucket-status",
                    style={"color": "#6B7280", "fontSize": "0.9rem", "marginTop": "8px"},
                ),
            ],
            style={
                "marginBottom": "18px",
                "padding": "12px",
                "border": "1px solid #E5E7EB",
                "borderRadius": "12px",
            },
        ),

        html.Button("Generate Deck", id="go", n_clicks=0, style={"padding":"10px 16px","borderRadius":"10px"}),
        html.Div(id="status", style={"marginTop":"10px", "color":"#888"}),
        dcc.Download(id="download"),
    ]
)

@callback(
    Output("data-upload-status", "children"),
    Input("data-upload", "contents"),
    State("data-upload", "filename"),
)
def show_data_upload_status(contents, filename):
    if not contents:
        return render_upload_status(None, "gatheredCN10 upload complete")
    return render_upload_status(filename, "gatheredCN10 upload complete")

@callback(
    Output("scope-upload-status", "children"),
    Input("scope-upload", "contents"),
    State("scope-upload", "filename"),
)
def show_scope_upload_status(contents, filename):
    if not contents:
        return render_upload_status(None, "Scope upload complete")
    return render_upload_status(filename, "Scope upload complete")

@callback(
    Output("waterfall-targets", "options"),
    Output("waterfall-targets", "value"),
    Output("waterfall-targets-status", "children"),
    Input("data-upload", "contents"),
    State("data-upload", "filename"),
)
def populate_waterfall_targets(contents, filename):
    if not contents:
        return [], [], "Upload a gatheredCN10 file to load Target Level Label values."
    try:
        gathered_df = df_from_contents(contents, filename)
    except Exception as exc:
        return [], [], f"Error reading gatheredCN10 file: {exc}"
    if gathered_df is None or gathered_df.empty:
        return [], [], "The gatheredCN10 file is empty."
    try:
        target_labels = _target_level_labels_from_gathered_df(gathered_df)
    except Exception as exc:
        return [], [], f"Error finding Target Level Label values: {exc}"
    if not target_labels:
        return [], [], "No Target Level Label values were found in the gatheredCN10 file."
    options = [{"label": label, "value": label} for label in target_labels]
    return options, target_labels, f"Found {len(target_labels)} Target Level Label value(s)."


@callback(
    Output("bucket-metadata", "data"),
    Output("bucket-year1", "options"),
    Output("bucket-year1", "value"),
    Output("bucket-year2", "options"),
    Output("bucket-year2", "value"),
    Output("bucket-group-controls", "children"),
    Output("bucket-status", "children"),
    Input("data-upload", "contents"),
    State("data-upload", "filename"),
)
def populate_bucket_controls(contents, filename):
    if not contents:
        return (
            {},
            [],
            None,
            [],
            None,
            [],
            "Upload a gatheredCN10 file to configure bucket inputs.",
        )
    try:
        raw_df = raw_df_from_contents(contents, filename)
        data_df, metadata = _parse_two_row_header_dataframe(raw_df)
    except Exception as exc:
        return (
            {},
            [],
            None,
            [],
            None,
            [],
            f"Error parsing two-row headers: {exc}",
        )

    if not metadata.get("target_label_id"):
        return (
            metadata,
            [],
            None,
            [],
            None,
            [],
            "The gatheredCN10 file is missing the Target Label column.",
        )
    if not metadata.get("year_id"):
        return (
            metadata,
            [],
            None,
            [],
            None,
            [],
            "The gatheredCN10 file is missing the Year column.",
        )

    year_values = _unique_column_values(data_df, metadata["year_id"])
    year_options = [{"label": value, "value": value} for value in year_values]
    year1_default = year_values[0] if year_values else None
    year2_default = year_values[1] if len(year_values) > 1 else year1_default

    column_labels: dict[str, str] = {}
    column_groups: dict[str, str] = {}
    group_controls = []
    for group in metadata.get("group_order", []):
        columns = metadata.get("groups", {}).get(group, [])
        if not columns:
            continue
        label_counts: dict[str, int] = {}
        column_rows = []
        for column in columns:
            subheader = column.get("subheader") or "Unnamed"
            label_counts[subheader] = label_counts.get(subheader, 0) + 1
            label = subheader
            if label_counts[subheader] > 1:
                label = f"{subheader} ({label_counts[subheader]})"
            column_id = column["id"]
            column_labels[column_id] = label
            column_groups[column_id] = group
            column_rows.append(
                html.Div(
                    [
                        dcc.Checklist(
                            id={"type": "bucket-column", "column": column_id},
                            options=[{"label": label, "value": column_id}],
                            value=[column_id],
                            labelStyle={"display": "block", "marginBottom": "4px"},
                            inputStyle={"marginRight": "6px"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "space-between",
                        "gap": "12px",
                        "marginBottom": "6px",
                    },
                )
            )
        group_controls.append(
            html.Div(
                [
                    html.Label(group, style={"fontWeight": "600"}),
                    html.Label(
                        "Target Label filter",
                        style={"fontSize": "0.85rem", "color": "#6B7280"},
                    ),
                    dcc.Checklist(
                        id={"type": "bucket-group-type", "group": group},
                        options=[
                            {"label": "Own", "value": "Own"},
                            {"label": "Cross", "value": "Cross"},
                        ],
                        value=["Own", "Cross"],
                        labelStyle={"display": "block", "marginBottom": "2px"},
                        inputStyle={"marginRight": "6px"},
                        style={"minWidth": "120px", "marginBottom": "8px"},
                    ),
                    *column_rows,
                ],
                style={
                    "marginBottom": "12px",
                    "padding": "8px",
                    "border": "1px solid #E5E7EB",
                    "borderRadius": "8px",
                },
            )
        )

    status = (
        f"Loaded {len(metadata.get('group_order', []))} bucket group(s)."
        if metadata.get("group_order")
        else "No bucket groups were found in the first header row."
    )
    return (
        {**metadata, "column_labels": column_labels, "column_groups": column_groups},
        year_options,
        year1_default,
        year_options,
        year2_default,
        group_controls,
        status,
    )


@callback(
    Output("bucket-config", "data"),
    Output("bucket-deltas", "data"),
    Output("bucket-status", "children", allow_duplicate=True),
    Input("apply-buckets", "n_clicks"),
    State("data-upload", "contents"),
    State("data-upload", "filename"),
    State("bucket-year1", "value"),
    State("bucket-year2", "value"),
    State("bucket-metadata", "data"),
    State({"type": "bucket-column", "column": ALL}, "value"),
    State({"type": "bucket-column", "column": ALL}, "id"),
    State({"type": "bucket-group-type", "group": ALL}, "value"),
    State({"type": "bucket-group-type", "group": ALL}, "id"),
    prevent_initial_call=True,
)
def apply_bucket_selection(
    n_clicks,
    contents,
    filename,
    year1,
    year2,
    metadata,
    selections,
    selection_ids,
    bucket_types,
    bucket_type_ids,
):
    if not contents:
        return no_update, no_update, "Upload a gatheredCN10 file before applying buckets."
    if not metadata:
        return (
            no_update,
            no_update,
            "Bucket metadata is unavailable. Re-upload the gatheredCN10 file.",
        )
    if not year1 or not year2:
        return no_update, no_update, "Select Year 1 and Year 2 values before applying."

    try:
        raw_df = raw_df_from_contents(contents, filename)
        data_df, parsed_meta = _parse_two_row_header_dataframe(raw_df)
    except Exception as exc:
        return no_update, no_update, f"Error parsing gatheredCN10: {exc}"

    bucket_type_map: dict[str, list[str]] = {}
    for bucket_type, type_id in zip(bucket_types, bucket_type_ids):
        group = type_id.get("group")
        if group:
            bucket_type_map[group] = [value for value in bucket_type or [] if value]

    selected_columns = []
    for selection, selection_id in zip(selections, selection_ids):
        column_id = selection_id.get("column")
        if column_id and selection:
            selected_columns.append(column_id)

    if not selected_columns:
        return no_update, no_update, "Select at least one bucket column before applying."

    column_groups = metadata.get("column_groups", {}) if metadata else {}
    group_columns: dict[str, list[str]] = {}
    for column_id in selected_columns:
        group = column_groups.get(column_id)
        if group:
            group_columns.setdefault(group, []).append(column_id)

    if not group_columns:
        return no_update, no_update, "Select at least one bucket column before applying."

    bucket_config: dict[str, dict[str, list[str]]] = {}
    for group in metadata.get("group_order", []):
        selected_group_columns = group_columns.get(group, [])
        if not selected_group_columns:
            continue
        target_labels = bucket_type_map.get(group)
        if target_labels is None:
            target_labels = ["Own", "Cross"]
        bucket_config[group] = {
            "target_labels": target_labels,
            "subheaders_included": selected_group_columns,
        }

    if not bucket_config:
        return no_update, no_update, "Select at least one bucket column before applying."

    try:
        deltas = _compute_bucket_deltas(
            data_df,
            parsed_meta,
            bucket_config,
            year1,
            year2,
        )
    except Exception as exc:
        return no_update, no_update, f"Error computing bucket deltas: {exc}"

    labels = [group for group, _ in deltas]
    values = [value for _, value in deltas]

    bucket_data = {
        "labels": labels,
        "values": values,
        "year1": year1,
        "year2": year2,
    }
    return bucket_config, bucket_data, f"Applied {len(values)} bucket delta(s) to the waterfall."


@callback(
    Output("download","data"),
    Output("status","children"),
    Input("go","n_clicks"),
    State("data-upload","contents"),
    State("data-upload","filename"),
    State("scope-upload", "contents"),
    State("scope-upload", "filename"),
    State("project-select", "value"),
    State("waterfall-targets", "value"),
    State("bucket-deltas", "data"),
    prevent_initial_call=True
)
def generate_deck(
    n_clicks,
    data_contents,
    data_name,
    scope_contents,
    scope_name,
    project_name,
    waterfall_targets,
    bucket_data,
):
    if not data_contents or not project_name:
        return no_update, "Please upload the data file and select a project."

    template_path = PROJECT_TEMPLATES.get(project_name)
    if not template_path or not template_path.exists():
        return no_update, "The selected project template could not be found."
    try:
        df = df_from_contents(data_contents, data_name)
        scope_df = None
        product_description_df = None
        if scope_contents:
            try:
                scope_df = scope_df_from_contents(scope_contents, scope_name)
            except Exception:
                scope_df = None
            try:
                product_description_df = product_description_df_from_contents(
                    scope_contents, scope_name
                )
            except Exception:
                product_description_df = None
        target_brand = target_brand_from_scope_df(scope_df)
        template_bytes = template_path.read_bytes()

        pptx_bytes = build_pptx_from_template(
            template_bytes,
            df,
            target_brand,
            project_name,
            scope_df,
            product_description_df,
            waterfall_targets,
            bucket_data,
        )
        return dcc.send_bytes(lambda buff: buff.write(pptx_bytes), "deck.pptx"), "Building deck..."

    except Exception as exc:
        logger.exception("Deck generation failed.")
        message = str(exc).strip()
        if not message:
            message = "Unknown error. Check server logs for details."
        return no_update, f"Error ({type(exc).__name__}): {message}"

# Important: Dash's dcc.send_bytes expects a writer function; we provide inline:
def _writer(f):
    pass

# Patch: we pass a lambda that writes nothing (handled internally). To attach bytes, we can use:
# return dcc.send_bytes(lambda b: b.write(pptx_bytes), "deck.pptx")

# Fix the callback to use the writer properly:
@callback(
    Output("download","data", allow_duplicate=True),
    Input("status","children"),
    State("data-upload","contents"),
    prevent_initial_call=True
)
def finalize_download(status_text, data_contents):
    # This is a no-op; left for clarity in a larger app. In the minimal example above,
    # you can directly return the 'dcc.send_bytes' with the actual bytes.
    return no_update

if __name__ == "__main__":
    app.run(debug=True)

