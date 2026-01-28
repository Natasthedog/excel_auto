from __future__ import annotations

import io
import posixpath
import zipfile
import xml.etree.ElementTree as ET

import pandas as pd

from test_utils import build_deck_bytes, build_test_template

REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


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


def _series_values(chart_xml: bytes, series_name: str) -> list[float]:
    root = ET.fromstring(chart_xml)
    ns = {"c": "http://schemas.openxmlformats.org/drawingml/2006/chart"}
    for ser in root.findall(".//c:ser", ns):
        name_node = ser.find("c:tx/c:v", ns)
        if name_node is None:
            name_node = ser.find("c:tx/c:strRef/c:strCache/c:pt/c:v", ns)
        if name_node is None or name_node.text != series_name:
            continue
        cache = ser.find("c:val/c:numRef/c:numCache", ns)
        if cache is None:
            return []
        values = []
        for pt in cache.findall("c:pt", ns):
            value_node = pt.find("c:v", ns)
            if value_node is None:
                continue
            values.append(float(value_node.text))
        return values
    raise AssertionError(f"Series {series_name} not found in chart XML.")


def test_waterfall_chart_uses_target_level_label_data(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=2)

    rows = []
    for label, base_value, pos_value, neg_value, blank_value in [
        ("Alpha", 100, 10, -5, 100),
        ("Beta", 200, 20, -10, 200),
    ]:
        for idx, category in enumerate(["Base", "Change", "Total"]):
            rows.append(
                {
                    "Target Level Label": label,
                    "Vars": category,
                    "Base": [base_value, 0, base_value + 10][idx],
                    "Promo": 0,
                    "Media": 0,
                    "Blanks": [0, blank_value, blank_value + 5][idx],
                    "Positives": [0, pos_value, 0][idx],
                    "Negatives": [0, neg_value, 0][idx],
                }
            )
    df = pd.DataFrame(rows)

    pptx_bytes = build_deck_bytes(
        template_path,
        df,
        waterfall_targets=["Alpha", "Beta"],
    )

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        slide_alpha = _slide_name_for_label(zf, "Alpha")
        slide_beta = _slide_name_for_label(zf, "Beta")
        alpha_targets = _chart_targets_for_slide(zf, slide_alpha)
        beta_targets = _chart_targets_for_slide(zf, slide_beta)
        alpha_chart_xml = zf.read(alpha_targets[0])
        beta_chart_xml = zf.read(beta_targets[0])

    alpha_values = _series_values(alpha_chart_xml, "Base")
    beta_values = _series_values(beta_chart_xml, "Base")

    assert alpha_values == [100.0, 0.0, 110.0]
    assert beta_values == [200.0, 0.0, 210.0]
