from __future__ import annotations

import io
import posixpath
import zipfile
import xml.etree.ElementTree as ET

from test_utils import build_deck_bytes, build_sample_dataframe, build_test_template

REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _slide_name_for_marker(zf: zipfile.ZipFile, marker: str) -> str:
    slide_files = [
        name
        for name in zf.namelist()
        if name.startswith("ppt/slides/slide") and name.endswith(".xml")
    ]
    for name in slide_files:
        if marker in zf.read(name).decode("utf-8", errors="ignore"):
            return name
    raise AssertionError(f"Marker {marker} not found in template slides.")


def _chart_part_target(zf: zipfile.ZipFile, slide_name: str) -> tuple[str, str]:
    rels_name = f"ppt/slides/_rels/{slide_name.rsplit('/', 1)[-1]}.rels"
    rels_xml = zf.read(rels_name)
    root = ET.fromstring(rels_xml)
    for rel in root.findall(f"{{{REL_NS}}}Relationship"):
        rel_type = rel.attrib.get("Type", "")
        if rel_type.endswith("/chart"):
            target = rel.attrib["Target"]
            chart_part = posixpath.normpath(posixpath.join("ppt/slides", target))
            return chart_part, rels_name
    raise AssertionError(f"No chart relationship found in {rels_name}.")


def _force_shared_chart_part(
    template_bytes: bytes,
    marker_primary: str,
    marker_secondary: str,
) -> bytes:
    with zipfile.ZipFile(io.BytesIO(template_bytes)) as zf:
        files = {name: zf.read(name) for name in zf.namelist()}
        slide_primary = _slide_name_for_marker(zf, marker_primary)
        slide_secondary = _slide_name_for_marker(zf, marker_secondary)

    with zipfile.ZipFile(io.BytesIO(template_bytes)) as zf:
        primary_target, _ = _chart_part_target(zf, slide_primary)
        secondary_target, secondary_rels_name = _chart_part_target(zf, slide_secondary)
        if primary_target == secondary_target:
            return template_bytes
        rels_xml = zf.read(secondary_rels_name)

    root = ET.fromstring(rels_xml)
    updated = False
    for rel in root.findall(f"{{{REL_NS}}}Relationship"):
        rel_type = rel.attrib.get("Type", "")
        if rel_type.endswith("/chart"):
            rel.attrib["Target"] = posixpath.relpath(primary_target, "ppt/slides")
            updated = True
            break
    if not updated:
        raise AssertionError(f"No chart relationship found in {secondary_rels_name}.")

    ET.register_namespace("", REL_NS)
    files[secondary_rels_name] = ET.tostring(
        root,
        encoding="utf-8",
        xml_declaration=True,
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return output.getvalue()


def test_waterfall_charts_are_independent_after_update(tmp_path) -> None:
    labels = ["Alpha", "Beta"]
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=len(labels))

    shared_bytes = _force_shared_chart_part(
        template_path.read_bytes(),
        "<Waterfall Template>",
        "<Waterfall Template2>",
    )
    template_path.write_bytes(shared_bytes)

    df = build_sample_dataframe(labels, include_brand=False)
    pptx_bytes = build_deck_bytes(template_path, df, waterfall_targets=labels)

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        slide_primary = _slide_name_for_marker(zf, "Alpha")
        slide_secondary = _slide_name_for_marker(zf, "Beta")
        chart_primary, _ = _chart_part_target(zf, slide_primary)
        chart_secondary, _ = _chart_part_target(zf, slide_secondary)
        assert chart_primary != chart_secondary
        assert zf.read(chart_primary) != zf.read(chart_secondary)
