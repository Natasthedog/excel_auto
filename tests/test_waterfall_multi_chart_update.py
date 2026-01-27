from __future__ import annotations

import io
import posixpath
import zipfile
import xml.etree.ElementTree as ET

from test_utils import build_deck_bytes, build_sample_dataframe, build_test_template

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


def test_waterfall_updates_all_charts_on_slide(tmp_path) -> None:
    labels = ["Alpha", "Beta"]
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=len(labels), waterfall_chart_count=2)

    df = build_sample_dataframe(labels, include_brand=False)
    pptx_bytes = build_deck_bytes(template_path, df, waterfall_targets=labels)

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        slide_alpha = _slide_name_for_label(zf, "Alpha")
        slide_beta = _slide_name_for_label(zf, "Beta")
        alpha_targets = _chart_targets_for_slide(zf, slide_alpha)
        beta_targets = _chart_targets_for_slide(zf, slide_beta)

        assert len(alpha_targets) >= 2
        assert len(beta_targets) >= 2

        for idx in range(2):
            assert zf.read(alpha_targets[idx]) != zf.read(beta_targets[idx])
