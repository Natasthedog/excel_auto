from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from pptx import Presentation

from app import _payload_checksum
from app import compute_waterfall_payloads_for_all_labels
from app import _waterfall_chart_from_slide
from app import _to_jsonable
from test_utils import build_test_template


def _template_chart_from_path(path: Path):
    prs = Presentation(path)
    for slide in prs.slides:
        chart = _waterfall_chart_from_slide(slide, "Waterfall Template")
        if chart is not None:
            return chart
    raise AssertionError("Expected waterfall chart in template.")


def _series_values(payload, series_name: str) -> list[float]:
    for name, values in payload.series_values:
        if name == series_name:
            return values
    raise AssertionError(f"Series {series_name} not found in payload.")


def test_compute_waterfall_payloads_for_all_labels(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=1)
    template_chart = _template_chart_from_path(template_path)

    rows = []
    for label, base_value, pos_value, neg_value, blank_value in [
        ("Alpha", 100, 10, -5, 100),
        ("Beta", 200, 20, -10, 200),
        ("Gamma", 150, 15, -7, 150),
    ]:
        for idx, category in enumerate(["Base", "Change", "Total"]):
            rows.append(
                {
                    "Target Level Label": label,
                    "Vars": category,
                    "Base": [base_value, 0, base_value + pos_value][idx],
                    "Promo": 0,
                    "Media": 0,
                    "Blanks": [0, blank_value, blank_value + 5][idx],
                    "Positives": [0, pos_value, 0][idx],
                    "Negatives": [0, neg_value, 0][idx],
                }
            )
    df = pd.DataFrame(rows)

    payloads = compute_waterfall_payloads_for_all_labels(
        df,
        scope_df=None,
        bucket_data=None,
        template_chart=template_chart,
        target_labels=["Alpha", "Beta", "Gamma"],
    )

    assert set(payloads.keys()) == {"Alpha", "Beta", "Gamma"}

    for label, payload in payloads.items():
        assert len(payload.categories) == 3
        for _, values in payload.series_values:
            assert len(values) == len(payload.categories)

    alpha_base = _series_values(payloads["Alpha"], "Base")
    beta_base = _series_values(payloads["Beta"], "Base")
    gamma_base = _series_values(payloads["Gamma"], "Base")

    assert alpha_base != beta_base
    assert beta_base != gamma_base


def test_waterfall_payload_sanitizes_missing_values(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=1)
    template_chart = _template_chart_from_path(template_path)

    rows = []
    for idx, category in enumerate(["Base", "Change", "Total"]):
        rows.append(
            {
                "Target Level Label": "Alpha",
                "Vars": category,
                "Base": [100, 0, 110][idx],
                "Promo": 0,
                "Media": 0,
                "Blanks": [0, 100, 105][idx],
                "Positives": [0, 10, 0][idx],
                "Negatives": [0, -5, 0][idx],
            }
        )
    df = pd.DataFrame(rows)

    payloads = compute_waterfall_payloads_for_all_labels(
        df,
        scope_df=None,
        bucket_data={
            "labels": ["Price", "Distribution", "Mix"],
            "values": [None, float("nan"), 5],
            "year1": "2023",
            "year2": "2024",
        },
        template_chart=template_chart,
        target_labels=["Alpha"],
    )

    payload = payloads["Alpha"]
    for _, values in payload.series_values:
        assert all(value is not None and not pd.isna(value) for value in values)

    checksum = _payload_checksum(payload.series_values)
    assert isinstance(checksum, float)
    assert not pd.isna(checksum)


def test_waterfall_payloads_are_json_serializable(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=1)
    template_chart = _template_chart_from_path(template_path)

    rows = []
    for idx, category in enumerate(["Base", "Change", "Total"]):
        rows.append(
            {
                "Target Level Label": "Alpha",
                "Vars": category,
                "Base": [100, 0, 110][idx],
                "Promo": 0,
                "Media": 0,
                "Blanks": [0, 100, 105][idx],
                "Positives": [0, 10, 0][idx],
                "Negatives": [0, -5, 0][idx],
            }
        )
    df = pd.DataFrame(rows)

    payloads = compute_waterfall_payloads_for_all_labels(
        df,
        scope_df=None,
        bucket_data=None,
        template_chart=template_chart,
        target_labels=["Alpha"],
    )

    serialized = json.dumps(_to_jsonable(payloads))
    parsed = json.loads(serialized)
    assert parsed["Alpha"]["categories"] == ["Base", "Change", "Total"]
