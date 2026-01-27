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


def _build_year_dataframe(labels: list[str]) -> pd.DataFrame:
    rows = []
    for idx, label in enumerate(labels):
        for year, actuals in (("2022", 200 + idx * 10), ("2023", 230 + idx * 10)):
            rows.append(
                {
                    "Target Level Label": label,
                    "Target Label": "Own",
                    "Year": year,
                    "Actuals": actuals,
                }
            )
    return pd.DataFrame(rows)


def test_waterfall_updates_with_bucket_years(tmp_path) -> None:
    labels = ["Alpha", "Beta"]
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=len(labels))

    df = _build_year_dataframe(labels)
    pptx_bytes = build_deck_bytes(
        template_path,
        df,
        waterfall_targets=labels,
        bucket_data={"year1": "2022", "year2": "2023"},
    )

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        slide_alpha = _slide_name_for_label(zf, "Alpha")
        slide_beta = _slide_name_for_label(zf, "Beta")
        alpha_targets = _chart_targets_for_slide(zf, slide_alpha)
        beta_targets = _chart_targets_for_slide(zf, slide_beta)

        assert alpha_targets
        assert beta_targets
        assert zf.read(alpha_targets[0]) != zf.read(beta_targets[0])
