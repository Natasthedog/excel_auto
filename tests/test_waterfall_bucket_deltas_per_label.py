from __future__ import annotations

import io
import posixpath
import zipfile
import xml.etree.ElementTree as ET

import pandas as pd

from app import _parse_two_row_header_dataframe, build_pptx_from_template
from test_utils import build_test_template

REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CHART_NS = "http://schemas.openxmlformats.org/drawingml/2006/chart"


def _slide_name_for_label(zf: zipfile.ZipFile, label: str) -> str:
    slide_files = [
        name
        for name in zf.namelist()
        if name.startswith("ppt/slides/slide") and name.endswith(".xml")
    ]
    for name in slide_files:
        if label in zf.read(name).decode("utf-8", errors="ignore"):
            return name
    raise AssertionError(f"Label {label} not found in slide xml.")


def _chart_targets_for_slide(zf: zipfile.ZipFile, slide_name: str) -> list[str]:
    rels_name = f"ppt/slides/_rels/{slide_name.rsplit('/', 1)[-1]}.rels"
    rels_xml = zf.read(rels_name)
    root = ET.fromstring(rels_xml)
    targets = []
    for rel in root.findall(f"{{{REL_NS}}}Relationship"):
        rel_type = rel.attrib.get("Type", "")
        if rel_type.endswith("/chart"):
            target = rel.attrib["Target"]
            targets.append(posixpath.normpath(posixpath.join("ppt/slides", target)))
    return targets


def _chart_values(chart_xml: bytes) -> list[float]:
    root = ET.fromstring(chart_xml)
    values = []
    for value in root.findall(f".//{{{CHART_NS}}}numCache/{{{CHART_NS}}}pt/{{{CHART_NS}}}v"):
        try:
            values.append(float(value.text))
        except (TypeError, ValueError):
            continue
    return values


def test_waterfall_bucket_deltas_per_target_label(tmp_path) -> None:
    labels = ["Alpha", "Beta"]
    template_path = tmp_path / "template.pptx"
    build_test_template(
        template_path,
        waterfall_slide_count=len(labels),
        waterfall_categories=["<earliest date>", "Change", "<latest date>"],
    )

    gathered_rows = []
    for label, year1_value, year2_value in [
        ("Alpha", 200, 260),
        ("Beta", 150, 190),
    ]:
        for year, actuals in (("2022", year1_value), ("2023", year2_value)):
            gathered_rows.append(
                {
                    "Target Level Label": label,
                    "Target Label": "Own",
                    "Year": year,
                    "Actuals": actuals,
                }
            )
    gathered_df = pd.DataFrame(gathered_rows)

    raw_df = pd.DataFrame(
        [
            ["Promo", "Promo", "Target Label", "Target Level Label", "Year"],
            ["Feature", "Display", "Target Label", "Target Level Label", "Year"],
            [5, 6, "Own", "Alpha", "2022"],
            [60, 62, "Own", "Alpha", "2023"],
            [7, 8, "Own", "Beta", "2022"],
            [120, 117, "Own", "Beta", "2023"],
        ]
    )
    bucket_data_df, bucket_metadata = _parse_two_row_header_dataframe(raw_df)
    promo_columns = [
        column["id"] for column in bucket_metadata.get("groups", {}).get("Promo", [])
    ]
    bucket_config = {
        "Promo": {
            "target_labels": ["Own"],
            "subheaders_included": promo_columns,
        }
    }
    bucket_data = {"year1": "2022", "year2": "2023"}

    pptx_bytes = build_pptx_from_template(
        template_path.read_bytes(),
        gathered_df,
        target_brand=None,
        project_name="MMx",
        scope_df=None,
        product_description_df=None,
        waterfall_targets=labels,
        bucket_data=bucket_data,
        bucket_config=bucket_config,
        bucket_metadata=bucket_metadata,
        bucket_data_df=bucket_data_df,
        modelled_in_value="US",
        metric_value="Units",
    )

    expected_deltas = {"Alpha": 111.0, "Beta": 222.0}

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        for label in labels:
            slide_name = _slide_name_for_label(zf, label)
            targets = _chart_targets_for_slide(zf, slide_name)
            assert targets
            values = _chart_values(zf.read(targets[0]))
            assert expected_deltas[label] in values
            other_label = "Beta" if label == "Alpha" else "Alpha"
            assert expected_deltas[other_label] not in values
