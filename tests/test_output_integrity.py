from __future__ import annotations

import io
import zipfile
import xml.etree.ElementTree as ET

from test_utils import build_deck_bytes, build_sample_dataframe, build_test_template


def test_output_integrity_and_relationships(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=1)

    df = build_sample_dataframe(["Alpha"], include_brand=False)
    pptx_bytes = build_deck_bytes(template_path, df, waterfall_targets=["Alpha"])

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        names = set(zf.namelist())
        for required in (
            "[Content_Types].xml",
            "ppt/presentation.xml",
            "ppt/_rels/presentation.xml.rels",
        ):
            assert required in names

        presentation_xml = zf.read("ppt/presentation.xml")
        rels_xml = zf.read("ppt/_rels/presentation.xml.rels")

    pres_root = ET.fromstring(presentation_xml)
    rels_root = ET.fromstring(rels_xml)
    ns = {
        "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    rel_ns = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}

    rels = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("r:Relationship", rel_ns)
    }

    slide_ids = pres_root.findall(".//p:sldId", ns)
    assert slide_ids

    for slide_id in slide_ids:
        rel_id = slide_id.attrib.get(f"{{{ns['r']}}}id")
        assert rel_id in rels
        target = rels[rel_id]
        assert f"ppt/{target}" in names
