from __future__ import annotations

import io
import zipfile
import xml.etree.ElementTree as ET

import pandas as pd

from test_utils import (
    build_deck_bytes,
    build_sample_dataframe,
    build_test_template,
    write_excel,
)


def _series_cache_points(root: ET.Element) -> list[tuple[int, int, int, int]]:
    ns = {"c": "http://schemas.openxmlformats.org/drawingml/2006/chart"}
    points = []
    for ser in root.findall(".//c:ser", ns):
        cat_cache = ser.find("c:cat/c:strRef/c:strCache", ns)
        if cat_cache is None:
            cat_cache = ser.find("c:cat/c:numRef/c:numCache", ns)
        num_cache = ser.find("c:val/c:numRef/c:numCache", ns)
        cat_pt_count = 0
        cat_pts = 0
        if cat_cache is not None:
            pt_count = cat_cache.find("c:ptCount", ns)
            if pt_count is not None:
                cat_pt_count = int(pt_count.attrib.get("val", "0"))
            cat_pts = len(cat_cache.findall("c:pt", ns))
        num_pt_count = 0
        num_pts = 0
        if num_cache is not None:
            pt_count = num_cache.find("c:ptCount", ns)
            if pt_count is not None:
                num_pt_count = int(pt_count.attrib.get("val", "0"))
            num_pts = len(num_cache.findall("c:pt", ns))
        points.append((cat_pt_count, cat_pts, num_pt_count, num_pts))
    return points


def test_waterfall_labels_render_without_edit_data(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=1)

    df = build_sample_dataframe(["Alpha"], include_brand=False)
    excel_path = tmp_path / "input.xlsx"
    write_excel(df, excel_path)
    df_from_excel = pd.read_excel(excel_path)
    pptx_bytes = build_deck_bytes(template_path, df_from_excel, waterfall_targets=["Alpha"])

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        chart_files = [
            name
            for name in zf.namelist()
            if name.startswith("ppt/charts/chart") and name.endswith(".xml")
        ]
        assert chart_files
        chart_xml = zf.read(chart_files[0])

    root = ET.fromstring(chart_xml)
    ns = {"c": "http://schemas.openxmlformats.org/drawingml/2006/chart"}
    d_lbls = root.findall(".//c:dLbls", ns)
    assert d_lbls

    label_flags = [
        lbl.find("c:showVal", ns) is not None or lbl.find("c:showCatName", ns) is not None
        for lbl in d_lbls
    ]
    assert any(label_flags)

    cache_points = _series_cache_points(root)
    assert cache_points
    for cat_pt_count, cat_pts, num_pt_count, num_pts in cache_points:
        assert cat_pt_count > 0
        assert cat_pts > 0
        assert num_pt_count > 0
        assert num_pts > 0
