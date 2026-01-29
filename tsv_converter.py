# In _build_waterfall_chart_data (line 2882-2894)
if gathered_df is not None and target_level_label:
    try:
        gathered_override = _waterfall_series_from_gathered_df(
            gathered_df,  # Pass full dataframe
            scope_df,
            target_level_label,  # Filter by this label
        )
    except Exception as exc:
        logger.info(
            "Skipping gatheredCN10 waterfall data for %r: %s",
            target_level_label,
            exc,
        )
        gathered_override = None


pip install pandas openpyxl
python tsv_to_xlsx.py "D:\path\to\your\folder"
