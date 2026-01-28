# In populate_category_waterfall (line 3282-3287)
resolved_label = _resolve_target_level_label_for_slide(slide, remaining_labels)
if resolved_label is None:
    if not remaining_labels:
        raise ValueError("No remaining Target Level Label values to assign.")
    resolved_label = remaining_labels[0]

# In populate_category_waterfall (line 3304-3311)
_update_waterfall_chart(
    slide,
    scope_df,
    gathered_df,  # Full dataframe
    resolved_label,  # Specific label for filtering
    bucket_data,
)


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




