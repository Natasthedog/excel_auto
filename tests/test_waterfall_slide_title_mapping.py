from __future__ import annotations

import io
import zipfile

from test_utils import build_deck_bytes, build_sample_dataframe, build_test_template


def _slide_with_text(zf: zipfile.ZipFile, text: str) -> str:
    for name in zf.namelist():
        if not name.startswith("ppt/slides/slide") or not name.endswith(".xml"):
            continue
        if text in zf.read(name).decode("utf-8", errors="ignore"):
            return name
    raise AssertionError(f"Slide containing {text!r} not found.")


def test_waterfall_slide_titles_map_to_labels(tmp_path) -> None:
    labels = ["Alpha", "Beta"]
    titles = ["  beta ", "ALPHA"]
    template_path = tmp_path / "template.pptx"
    build_test_template(
        template_path,
        waterfall_slide_count=len(labels),
        waterfall_titles=titles,
    )

    df = build_sample_dataframe(labels, include_brand=False)
    pptx_bytes = build_deck_bytes(template_path, df, waterfall_targets=None)

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        slide_one = _slide_with_text(zf, "Template Style 1")
        slide_two = _slide_with_text(zf, "Template Style 2")
        slide_one_xml = zf.read(slide_one).decode("utf-8", errors="ignore")
        slide_two_xml = zf.read(slide_two).decode("utf-8", errors="ignore")
        assert "Label: Beta" in slide_one_xml
        assert "Label: Alpha" in slide_two_xml
