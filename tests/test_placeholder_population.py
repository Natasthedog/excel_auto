from __future__ import annotations

import io
import zipfile

from test_utils import build_deck_bytes, build_sample_dataframe, build_test_template

PLACEHOLDERS = ["<Target Level Label>", "<modelled in>", "<metric>"]


def test_placeholders_are_replaced(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=1)

    df = build_sample_dataframe(["Alpha"], include_brand=False)
    pptx_bytes = build_deck_bytes(
        template_path,
        df,
        waterfall_targets=["Alpha"],
        modelled_in_value="USA",
        metric_value="Volume",
    )

    with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
        slide_files = [
            name
            for name in zf.namelist()
            if name.startswith("ppt/slides/slide") and name.endswith(".xml")
        ]
        assert slide_files
        for slide in slide_files:
            content = zf.read(slide).decode("utf-8", errors="ignore")
            for placeholder in PLACEHOLDERS:
                assert placeholder not in content
