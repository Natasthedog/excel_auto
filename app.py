# app.py
import io
import base64
import pandas as pd
from dash import Dash, html, dcc, Input, Output, State, callback, no_update
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.chart.data import ChartData
from pptx.enum.chart import XL_CHART_TYPE

app = Dash(__name__)
app.title = "Deck Automator (MVP)"
server = app.server

def df_from_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    if filename.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(decoded))
    elif filename.lower().endswith(".csv"):
        return pd.read_csv(io.StringIO(decoded.decode('utf-8')))
    else:
        raise ValueError("Unsupported file format. Please upload CSV or Excel.")

def update_or_add_column_chart(slide, chart_name, categories, series_dict):
    """
    If a chart with name=chart_name exists on the slide, update its data.
    Otherwise insert a new clustered column chart in a sensible spot.
    Charts produced here remain EDITABLE in PowerPoint.
    """
    chart_shape = None
    for shape in slide.shapes:
        if hasattr(shape, "name") and shape.name == chart_name and shape.has_chart:
            chart_shape = shape
            break

    cd = ChartData()
    cd.categories = categories
    for s_name, values in series_dict.items():
        cd.add_series(s_name, list(values))

    if chart_shape:
        # Replace data in existing chart (preserves template styling)
        chart_shape.chart.replace_data(cd)
        return chart_shape
    else:
        # Insert a new chart (fallback)
        left, top, width, height = Inches(1), Inches(2), Inches(8), Inches(4.5)
        chart = slide.shapes.add_chart(
            XL_CHART_TYPE.COLUMN_CLUSTERED, left, top, width, height, cd
        ).chart
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

def add_table(slide, table_name, df: pd.DataFrame):
    # If there is a placeholder shape with that name and it's a table, try to fill it.
    for shape in slide.shapes:
        if getattr(shape, "name", None) == table_name and shape.has_table:
            tbl = shape.table
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
            return True

    # Otherwise, add a new table
    rows, cols = len(df) + 1, len(df.columns)
    left, top, width, height = Inches(1), Inches(1.5), Inches(8), Inches(1 + 0.3 * rows)
    table_shape = slide.shapes.add_table(rows, cols, left, top, width, height)
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

def build_pptx_from_template(template_bytes, df):
    prs = Presentation(io.BytesIO(template_bytes))
    # Assume Slide 1 has TitleBox & SubTitle
    slide1 = prs.slides[0]
    set_text_by_name(slide1, "TitleBox", "Monthly Performance Summary")
    set_text_by_name(slide1, "SubTitle", "Auto-generated via Dash + python-pptx")

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

    # Return bytes
    out = io.BytesIO()
    prs.save(out)
    out.seek(0)
    return out.read()

app.layout = html.Div(
    style={"maxWidth":"900px","margin":"40px auto","fontFamily":"Inter, system-ui"},
    children=[
        html.H2("PowerPoint Deck Automator (Dash + python-pptx)"),
        html.P("Upload your data and a PPTX template with named shapes (TitleBox, SubTitle, Table_Summary, Chart_ShareByBrand)."),
        html.Div([
            html.Label("Upload data (CSV/XLSX):"),
            dcc.Upload(id="data-upload", children=html.Div(["Drag & Drop or ", html.A("Select File")]),
                       multiple=False, style={"padding":"20px","border":"1px dashed #888","borderRadius":"12px","marginBottom":"12px"}),
            html.Label("Upload PPTX template:"),
            dcc.Upload(id="pptx-upload", children=html.Div(["Drag & Drop or ", html.A("Select PPTX")]),
                       multiple=False, style={"padding":"20px","border":"1px dashed #888","borderRadius":"12px"}),
        ], style={"marginBottom":"18px"}),

        html.Button("Generate Deck", id="go", n_clicks=0, style={"padding":"10px 16px","borderRadius":"10px"}),
        html.Div(id="status", style={"marginTop":"10px", "color":"#888"}),
        dcc.Download(id="download"),
    ]
)

@callback(
    Output("download","data"),
    Output("status","children"),
    Input("go","n_clicks"),
    State("data-upload","contents"),
    State("data-upload","filename"),
    State("pptx-upload","contents"),
    State("pptx-upload","filename"),
    prevent_initial_call=True
)
def generate_deck(n_clicks, data_contents, data_name, pptx_contents, pptx_name):
    if not data_contents or not pptx_contents:
        return no_update, "Please upload both the data file and the PPTX template."
    try:
        df = df_from_contents(data_contents, data_name)
        _, pptx_b64 = pptx_contents.split(',')
        template_bytes = base64.b64decode(pptx_b64)

        pptx_bytes = build_pptx_from_template(template_bytes, df)
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
    State("pptx-upload","contents"),
    State("data-upload","contents"),
    prevent_initial_call=True
)
def finalize_download(status_text, pptx_contents, data_contents):
    # This is a no-op; left for clarity in a larger app. In the minimal example above,
    # you can directly return the 'dcc.send_bytes' with the actual bytes.
    return no_update

if __name__ == "__main__":
    app.run(debug=True)
