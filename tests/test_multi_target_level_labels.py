from __future__ import annotations

import io
import zipfile

from test_utils import build_deck_bytes, build_sample_dataframe, build_test_template


def test_multi_target_level_labels_map_to_templates(tmp_path) -> None:
    labels = ["Alpha", "Beta"]
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=len(labels))

    df = build_sample_dataframe(labels, include_brand=False)
    pptx_bytes = build_deck_bytes(template_path, df, waterfall_targets=labels)

    with zipfile.ZipFile(template_path) as template_zip:
        template_slide_files = [
            name
            for name in template_zip.namelist()
            if name.startswith("ppt/slides/slide") and name.endswith(".xml")
        ]

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        slide_files = [
            name
            for name in zf.namelist()
            if name.startswith("ppt/slides/slide") and name.endswith(".xml")
        ]
        assert slide_files
        slide_texts = {name: zf.read(name).decode("utf-8", errors="ignore") for name in slide_files}

    for idx, label in enumerate(labels, start=1):
        style_text = f"Template Style {idx}"
        matching_slides = [
            name
            for name, content in slide_texts.items()
            if label in content and style_text in content
        ]
        assert matching_slides

    for marker in ("<Waterfall Template>", "<Waterfall Template2>"):
        assert not any(marker in content for content in slide_texts.values())

    assert len(slide_texts) == len(template_slide_files)
