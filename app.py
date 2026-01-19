# app.py
import io
import base64
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dash import Dash, html, dcc, Input, Output, State, callback, no_update
from pptx import Presentation
from pptx.util import Inches
from pptx.enum.text import PP_ALIGN
from pptx.chart.data import ChartData
from pptx.enum.chart import XL_CHART_TYPE

app = Dash(__name__)
app.title = "Deck Automator (MVP)"
server = app.server
TEMPLATE_DIR = Path(__file__).resolve().parent / "assets" / "templates"
PROJECT_TEMPLATES = {
    "PnP": TEMPLATE_DIR / "PnP.pptx",
    "MMx": TEMPLATE_DIR / "MMx.pptx",
    "MMM": TEMPLATE_DIR / "MMM.pptx",
}


@dataclass(frozen=True)
class CompanyWeekMapper:
    """
    Maps between a sequential internal 'company_week' integer and YEARWK (YYYYWW),
    assuming YEARWK is an ISO week (ISO-8601).

    Provide at least one anchor mapping:
        (anchor_company_week -> anchor_yearwk)

    Optionally provide a second anchor to validate the mapping.
    """
    anchor_company_week: int
    anchor_yearwk: int
    check_company_week: int | None = None
    check_yearwk: int | None = None

    @staticmethod
    def _yearwk_to_monday(yearwk: int) -> date:
        y, w = divmod(yearwk, 100)
        if not (1 <= w <= 53):
            raise ValueError(f"Invalid YEARWK week number: {yearwk}")
        # Monday of ISO week
        return date.fromisocalendar(y, w, 1)

    @staticmethod
    def _monday_to_yearwk(d: date) -> int:
        iso_y, iso_w, _ = d.isocalendar()
        return iso_y * 100 + iso_w

    def __post_init__(self):
        # Optional consistency check with second anchor
        if (self.check_company_week is None) ^ (self.check_yearwk is None):
            raise ValueError("Provide both check_company_week and check_yearwk, or neither.")

        if self.check_company_week is not None:
            a_date = self._yearwk_to_monday(self.anchor_yearwk)
            delta = self.check_company_week - self.anchor_company_week
            derived = self._monday_to_yearwk(a_date + timedelta(weeks=delta))
            if derived != self.check_yearwk:
                raise ValueError(
                    f"Anchors inconsistent: derived {derived} but expected {self.check_yearwk}."
                )

    def to_yearwk(self, company_week: int) -> int:
        a_date = self._yearwk_to_monday(self.anchor_yearwk)
        delta_weeks = company_week - self.anchor_company_week
        out_date = a_date + timedelta(weeks=delta_weeks)
        return self._monday_to_yearwk(out_date)

    def to_company_week(self, yearwk: int) -> int:
        a_date = self._yearwk_to_monday(self.anchor_yearwk)
        target_date = self._yearwk_to_monday(yearwk)
        delta_days = (target_date - a_date).days

        if delta_days % 7 != 0:
            # Should never happen because both are Mondays, but keep it safe
            raise ValueError("Non-week-aligned difference; check inputs.")
        delta_weeks = delta_days // 7
        return self.anchor_company_week + delta_weeks

def bytes_from_contents(contents):
    _, content_string = contents.split(',')
    return base64.b64decode(content_string)


def df_from_contents(contents, filename):
    decoded = bytes_from_contents(contents)
    if filename.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(decoded))
    elif filename.lower().endswith(".csv"):
        return pd.read_csv(io.StringIO(decoded.decode('utf-8')))
    else:
        raise ValueError("Unsupported file format. Please upload CSV or Excel.")


def scope_df_from_contents(contents, filename):
    if not filename or not filename.lower().endswith((".xlsx", ".xlsb")):
        raise ValueError("Scope file must be an Excel workbook (.xlsx or .xlsb).")

    decoded = bytes_from_contents(contents)
    read_options = {}
    if filename.lower().endswith(".xlsb"):
        read_options["engine"] = "pyxlsb"
    scope_df = pd.read_excel(
        io.BytesIO(decoded),
        sheet_name="Overall Information",
        **read_options,
    )
    if scope_df.empty:
        return None
    return scope_df


def target_brand_from_scope_df(scope_df):
    if scope_df is None or scope_df.empty:
        return None

    column_lookup = {str(col).strip().lower(): col for col in scope_df.columns}
    if "target brand" in column_lookup:
        series = scope_df[column_lookup["target brand"]].dropna()
        if not series.empty:
            return str(series.iloc[0])

    for _, row in scope_df.iterrows():
        if not len(row):
            continue
        label = str(row.iloc[0]).strip().lower()
        normalized_label = label.rstrip(":")
        if normalized_label == "target brand" and len(row) > 1 and pd.notna(row.iloc[1]):
            return str(row.iloc[1])

    return None

def update_or_add_column_chart(slide, chart_name, categories, series_dict):
    """
    If a chart with name=chart_name exists on the slide, update its data.
    Otherwise insert a new clustered column chart in a sensible spot.
    Charts produced here remain EDITABLE in PowerPoint.
    """
    chart_shape = None
    for shape in slide.shapes:
        if getattr(shape, "name", None) == chart_name:
            if shape.has_chart:
                chart_shape = shape
                break
            else:
                # Remove placeholder artifacts that aren't real charts
                sp = shape._element
                sp.getparent().remove(sp)

    cd = ChartData()
    cd.categories = categories
    for s_name, values in series_dict.items():
        cd.add_series(s_name, list(values))

    if chart_shape:
        # Replace data in existing chart (preserves template styling)
        chart_shape.chart.replace_data(cd)
        return chart_shape
    else:
        # Fallback: repurpose the first chart on the slide if present.
        for shape in slide.shapes:
            if shape.has_chart:
                shape.chart.replace_data(cd)
                shape.name = chart_name
                return shape
        # Remove any stale shapes with the target name before adding a new chart
        for shape in list(slide.shapes):
            if getattr(shape, "name", None) == chart_name:
                sp = shape._element
                sp.getparent().remove(sp)

        # Insert a new chart (fallback)
        left, top, width, height = Inches(1), Inches(2), Inches(8), Inches(4.5)
        chart_shape = slide.shapes.add_chart(
            XL_CHART_TYPE.COLUMN_CLUSTERED, left, top, width, height, cd
        )
        chart_shape.name = chart_name
        chart = chart_shape.chart
        # Light touch formatting; rely on template/theme for styling
        chart.has_legend = True
        return chart

def set_text_by_name(slide, shape_name, text):
    for shape in slide.shapes:
        if getattr(shape, "name", None) == shape_name and shape.has_text_frame:
            tf = shape.text_frame
            tf.clear()
            p = tf.paragraphs[0]
            run = p.add_run()
            run.text = str(text)
            p.alignment = PP_ALIGN.LEFT
            return True
    return False


def replace_text_in_slide(slide, old_text, new_text):
    replaced = False
    for shape in slide.shapes:
        if not shape.has_text_frame:
            continue
        text_frame = shape.text_frame
        current_text = text_frame.text
        if old_text in current_text:
            text_frame.text = current_text.replace(old_text, new_text)
            replaced = True
    return replaced

def add_table(slide, table_name, df: pd.DataFrame):
    # Identify an existing table to reuse, preferring one with the expected name.
    target_shape = None
    for shape in slide.shapes:
        if getattr(shape, "name", None) == table_name and shape.has_table:
            target_shape = shape
            break

    if target_shape is None:
        for shape in slide.shapes:
            if shape.has_table:
                target_shape = shape
                target_shape.name = table_name
                break

    if target_shape and target_shape.has_table:
        tbl = target_shape.table
        # Resize (simple): write headers to row 0, then rows afterward if room allows
        n_rows = min(len(df) + 1, tbl.rows.__len__())
        n_cols = min(len(df.columns), tbl.columns.__len__())
        # headers
        for j, col in enumerate(df.columns[:n_cols]):
            cell = tbl.cell(0, j)
            cell.text = str(col)
        # cells
        for i in range(1, n_rows):
            for j in range(n_cols):
                tbl.cell(i, j).text = str(df.iloc[i-1, j])
        # Clear any leftover rows beyond the populated range
        for i in range(n_rows, tbl.rows.__len__()):
            for j in range(tbl.columns.__len__()):
                tbl.cell(i, j).text = ""
        return True

    # Remove non-table placeholders with the desired name so we can insert a fresh table.
    for shape in list(slide.shapes):
        if getattr(shape, "name", None) == table_name and not shape.has_table:
            sp = shape._element
            sp.getparent().remove(sp)

    # Otherwise, add a new table
    rows, cols = len(df) + 1, len(df.columns)
    left, top, width, height = Inches(1), Inches(1.5), Inches(8), Inches(1 + 0.3 * rows)
    table_shape = slide.shapes.add_table(rows, cols, left, top, width, height)
    table_shape.name = table_name
    table = table_shape.table
    for j, col in enumerate(df.columns):
        table.cell(0, j).text = str(col)
    for i in range(len(df)):
        for j in range(len(df.columns)):
            table.cell(i+1, j).text = str(df.iloc[i, j])

    # Avoid manipulating the low-level XML that may not exist across templates.
    # python-pptx represents the table as a ``CT_GraphicalObjectFrame`` whose
    # schema does not expose a ``graphicFrame`` attribute.  Some versions of the
    # library can therefore raise an AttributeError when we try to clear borders
    # by touching ``graphicFrame`` directly.  Since this styling tweak is only a
    # nice-to-have, we simply rely on the template/theme defaults instead of
    # editing the XML manually.
    return True

def remove_empty_placeholders(slide):
    """Remove placeholder shapes that have no meaningful content."""
    for shape in list(slide.shapes):
        if not getattr(shape, "is_placeholder", False):
            continue

        # Keep placeholders that now contain text, tables, or charts with data.
        if shape.has_text_frame:
            if shape.text_frame.text and shape.text_frame.text.strip():
                continue
        elif shape.has_table:
            # If every cell is blank, treat as empty.
            if any(
                cell.text.strip()
                for row in shape.table.rows
                for cell in row.cells
            ):
                continue
        elif shape.has_chart:
            # Assume populated charts should remain.
            continue

        sp = shape._element
        sp.getparent().remove(sp)


def _normalize_label(value: str) -> str:
    return " ".join(value.strip().lower().replace(":", "").split())


def _find_company_week_value(scope_df: pd.DataFrame, label: str):
    if scope_df is None or scope_df.empty or scope_df.shape[1] < 2:
        raise ValueError("Scope file must include labels in column A and company weeks in column B.")
    label_normalized = _normalize_label(label)
    candidates = []
    for _, row in scope_df.iterrows():
        cell_value = row.iloc[0]
        if pd.isna(cell_value):
            continue
        cell_label = _normalize_label(str(cell_value))
        candidates.append((cell_label, row))
        if label_normalized in cell_label or cell_label in label_normalized:
            value = row.iloc[1]
            if pd.isna(value):
                raise ValueError(f"Missing company week value for '{label}'.")
            return value
    if candidates:
        from difflib import get_close_matches

        labels = [candidate_label for candidate_label, _ in candidates]
        matches = get_close_matches(label_normalized, labels, n=1, cutoff=0.7)
        if matches:
            matched_label = matches[0]
            for candidate_label, row in candidates:
                if candidate_label == matched_label:
                    value = row.iloc[1]
                    if pd.isna(value):
                        raise ValueError(f"Missing company week value for '{label}'.")
                    return value
    raise ValueError(f"Could not find '{label}' in the scope file.")


def _coerce_yearwk(value) -> int:
    if pd.isna(value):
        raise ValueError("Missing company week value for modelling period.")
    raw = str(value).strip()
    if not raw:
        raise ValueError("Missing company week value for modelling period.")
    try:
        yearwk = int(float(raw))
    except ValueError:
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            raise ValueError("Company week value must be a YYYYWW-style week number.")
        yearwk = int(digits)
    year, week = divmod(yearwk, 100)
    if year <= 0 or not (1 <= week <= 53):
        raise ValueError("Company week value must be a YYYYWW-style week number.")
    return yearwk


def _format_modelling_period(data_df: pd.DataFrame, scope_df: pd.DataFrame) -> str:
    start_company_week = _find_company_week_value(scope_df, "First week of modelling")
    end_company_week = _find_company_week_value(scope_df, "Last week of modelling")
    start_yearwk = _coerce_yearwk(start_company_week)
    end_yearwk = _coerce_yearwk(end_company_week)
    start_date = CompanyWeekMapper._yearwk_to_monday(start_yearwk)
    end_date = CompanyWeekMapper._yearwk_to_monday(end_yearwk) + timedelta(days=6)
    return f"{start_date:%b %d, %Y} - {end_date:%b %d, %Y}"

def build_pptx_from_template(
    template_bytes,
    df,
    target_brand=None,
    project_name=None,
    scope_df=None,
):
    prs = Presentation(io.BytesIO(template_bytes))
    # Assume Slide 1 has TitleBox & SubTitle
    slide1 = prs.slides[0]
    set_text_by_name(slide1, "TitleBox", "Monthly Performance Summary")
    set_text_by_name(slide1, "SubTitle", "Auto-generated via Dash + python-pptx")
    if target_brand:
        replace_text_in_slide(slide1, "Target Brand", target_brand)
    remove_empty_placeholders(slide1)

    # Assume Slide 2 is for a KPI table and a chart
    slide2 = prs.slides[1] if len(prs.slides) > 1 else prs.slides.add_slide(prs.slide_layouts[5])

    # Simple KPIs (example): top 5 brands by value
    if "Brand" in df.columns and "Value" in df.columns:
        kpis = (
            df.groupby("Brand", as_index=False)["Value"].sum()
              .sort_values("Value", ascending=False)
              .head(5)
        )
        add_table(slide2, "Table_Summary", kpis)

        # Chart: share by Brand (editable)
        categories = kpis["Brand"].tolist()
        series = {"Value": kpis["Value"].tolist()}
        update_or_add_column_chart(slide2, "Chart_ShareByBrand", categories, series)

    remove_empty_placeholders(slide2)

    if project_name == "MMx" and len(prs.slides) > 3:
        slide4 = prs.slides[3]
        time_period = _format_modelling_period(df, scope_df)
        replace_text_in_slide(slide4, "TIME PERIOD", f"TIME PERIOD\n{time_period}")
        remove_empty_placeholders(slide4)

    # Return bytes
    out = io.BytesIO()
    prs.save(out)
    out.seek(0)
    return out.read()

def render_upload_status(filename, success_label):
    if not filename:
        return html.Div("No file uploaded yet.", style={"color": "#888", "fontSize": "0.9rem"})

    return html.Div(
        [
            html.Div(
                style={
                    "height": "10px",
                    "backgroundColor": "#E5E7EB",
                    "borderRadius": "999px",
                    "overflow": "hidden",
                    "marginTop": "8px",
                },
                children=[
                    html.Div(
                        style={
                            "width": "100%",
                            "height": "100%",
                            "backgroundColor": "#22C55E",
                            "transition": "width 0.3s ease",
                        }
                    )
                ],
            ),
            html.Div(
                f"{success_label}: {filename}",
                style={"color": "#15803D", "fontSize": "0.9rem", "marginTop": "6px"},
            ),
        ]
    )

app.layout = html.Div(
    style={"maxWidth":"900px","margin":"40px auto","fontFamily":"Inter, system-ui"},
    children=[
        html.H2("PowerPoint Deck Automator (Dash + python-pptx)"),
        html.P("Upload your data, pick the project, and we will fill the matching PPTX template."),
        html.Div(
            [
                html.Label("Which project are you working on?"),
                dcc.Dropdown(
                    id="project-select",
                    options=[{"label": key, "value": key} for key in PROJECT_TEMPLATES],
                    placeholder="Select a project",
                    clearable=False,
                ),
            ],
            style={"marginBottom": "18px"},
        ),
        html.Div([
            html.Label("Upload data (CSV/XLSX):"),
            dcc.Upload(
                id="data-upload",
                children=html.Div(["Drag & Drop or ", html.A("Select File")]),
                multiple=False,
                style={
                    "padding":"20px",
                    "border":"1px dashed #888",
                    "borderRadius":"12px",
                    "marginBottom":"6px",
                },
            ),
            html.Div(
                id="data-upload-status",
                children=render_upload_status(None, "Data upload complete"),
                style={"marginBottom":"12px"},
            ),
        ], style={"marginBottom":"18px"}),
        html.Div([
            html.Label("Upload scope file (.xlsx or .xlsb):"),
            dcc.Upload(
                id="scope-upload",
                children=html.Div(["Drag & Drop or ", html.A("Select File")]),
                accept=".xlsx,.xlsb",
                multiple=False,
                style={
                    "padding":"20px",
                    "border":"1px dashed #888",
                    "borderRadius":"12px",
                    "marginBottom":"6px",
                },
            ),
            html.Div(
                id="scope-upload-status",
                children=render_upload_status(None, "Scope upload complete"),
                style={"marginBottom":"12px"},
            ),
        ], style={"marginBottom":"18px"}),

        html.Button("Generate Deck", id="go", n_clicks=0, style={"padding":"10px 16px","borderRadius":"10px"}),
        html.Div(id="status", style={"marginTop":"10px", "color":"#888"}),
        dcc.Download(id="download"),
    ]
)

@callback(
    Output("data-upload-status", "children"),
    Input("data-upload", "contents"),
    State("data-upload", "filename"),
)
def show_data_upload_status(contents, filename):
    if not contents:
        return render_upload_status(None, "Data upload complete")
    return render_upload_status(filename, "Data upload complete")

@callback(
    Output("scope-upload-status", "children"),
    Input("scope-upload", "contents"),
    State("scope-upload", "filename"),
)
def show_scope_upload_status(contents, filename):
    if not contents:
        return render_upload_status(None, "Scope upload complete")
    return render_upload_status(filename, "Scope upload complete")

@callback(
    Output("download","data"),
    Output("status","children"),
    Input("go","n_clicks"),
    State("data-upload","contents"),
    State("data-upload","filename"),
    State("scope-upload", "contents"),
    State("scope-upload", "filename"),
    State("project-select", "value"),
    prevent_initial_call=True
)
def generate_deck(
    n_clicks,
    data_contents,
    data_name,
    scope_contents,
    scope_name,
    project_name,
):
    if not data_contents or not project_name or not scope_contents:
        return no_update, "Please upload the data file, scope file, and select a project."

    template_path = PROJECT_TEMPLATES.get(project_name)
    if not template_path or not template_path.exists():
        return no_update, "The selected project template could not be found."
    try:
        df = df_from_contents(data_contents, data_name)
        scope_df = scope_df_from_contents(scope_contents, scope_name)
        target_brand = target_brand_from_scope_df(scope_df)
        template_bytes = template_path.read_bytes()

        pptx_bytes = build_pptx_from_template(
            template_bytes,
            df,
            target_brand,
            project_name,
            scope_df,
        )
        return dcc.send_bytes(lambda buff: buff.write(pptx_bytes), "deck.pptx"), "Building deck..."

    except Exception as e:
        return no_update, f"Error: {e}"

# Important: Dash's dcc.send_bytes expects a writer function; we provide inline:
def _writer(f):
    pass

# Patch: we pass a lambda that writes nothing (handled internally). To attach bytes, we can use:
# return dcc.send_bytes(lambda b: b.write(pptx_bytes), "deck.pptx")

# Fix the callback to use the writer properly:
@callback(
    Output("download","data", allow_duplicate=True),
    Input("status","children"),
    State("data-upload","contents"),
    prevent_initial_call=True
)
def finalize_download(status_text, data_contents):
    # This is a no-op; left for clarity in a larger app. In the minimal example above,
    # you can directly return the 'dcc.send_bytes' with the actual bytes.
    return no_update

if __name__ == "__main__":
    app.run(debug=True)
