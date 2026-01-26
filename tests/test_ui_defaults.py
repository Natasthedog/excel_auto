from __future__ import annotations

from dash.development.base_component import Component

from app import app as dash_app
from test_utils import build_deck_bytes, build_sample_dataframe, build_test_template


def _find_component_by_id(component: Component, component_id: str) -> Component | None:
    if getattr(component, "id", None) == component_id:
        return component

    children = getattr(component, "children", None)
    if isinstance(children, (list, tuple)):
        for child in children:
            found = _find_component_by_id(child, component_id)
            if found is not None:
                return found
    elif isinstance(children, Component):
        return _find_component_by_id(children, component_id)
    return None


def _prop(component: Component, name: str):
    return component.to_plotly_json().get("props", {}).get(name)


def test_ui_defaults_are_empty() -> None:
    layout = dash_app.layout

    project_select = _find_component_by_id(layout, "project-select")
    assert project_select is not None
    assert _prop(project_select, "value") is None

    waterfall_targets = _find_component_by_id(layout, "waterfall-targets")
    assert waterfall_targets is not None
    assert _prop(waterfall_targets, "value") == []
    assert _prop(waterfall_targets, "options") == []

    bucket_year1 = _find_component_by_id(layout, "bucket-year1")
    assert bucket_year1 is not None
    assert _prop(bucket_year1, "value") is None
    assert _prop(bucket_year1, "options") == []

    bucket_year2 = _find_component_by_id(layout, "bucket-year2")
    assert bucket_year2 is not None
    assert _prop(bucket_year2, "value") is None
    assert _prop(bucket_year2, "options") == []



def test_no_target_level_selection_still_builds_deck(tmp_path) -> None:
    template_path = tmp_path / "template.pptx"
    build_test_template(template_path, waterfall_slide_count=2)

    df = build_sample_dataframe(["Alpha", "Beta"])
    pptx_bytes = build_deck_bytes(template_path, df, waterfall_targets=[])

    assert pptx_bytes
