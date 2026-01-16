# Deck Automator

## Overview
This project is a Dash application that automates the creation of a PowerPoint marketing deck from a data file and a PPTX template. It lets you upload a CSV/XLSX dataset and a template deck, then generates a new presentation with updated titles, tables, and charts using `python-pptx`.

## What the app does
- Reads CSV or Excel data uploads.
- Loads a PowerPoint template.
- Updates slide 1 with a title and subtitle.
- Populates slide 2 with a KPI table and an editable column chart based on the top 5 brands by value.
- Removes empty placeholders so the deck looks clean.
- Returns a downloadable PPTX file via the Dash UI.

## Expected template shape names
To align with the automation logic, the PPTX template should include shapes with the following names:
- `TitleBox` (slide 1 title)
- `SubTitle` (slide 1 subtitle)
- `Table_Summary` (slide 2 KPI table)
- `Chart_ShareByBrand` (slide 2 column chart)

## Running locally
1. Install dependencies (Dash, pandas, python-pptx).
2. Run the app:
   ```bash
   python app.py
   ```
3. Open the local server URL in your browser, upload your data and PPTX template, then click **Generate Deck**.

## Project files
- `app.py`: Dash app and PPTX automation logic.
- `README.md`: Project overview and usage instructions.
