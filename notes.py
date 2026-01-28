def _parse_two_row_header_dataframe(
    raw_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """Parse a gatheredCN10 file that uses two header rows.

    Returns the data rows with stable internal column IDs plus metadata for UI mapping.
    """
    if raw_df is None or raw_df.empty or raw_df.shape[0] < 3:
        raise ValueError("The gatheredCN10 file must include two header rows and data rows.")
    header_row1 = raw_df.iloc[0].fillna("")
    header_row2 = raw_df.iloc[1].fillna("")
    columns_meta = []
    group_map: dict[str, list[dict]] = {}
    group_order: list[str] = []
    for idx in range(raw_df.shape[1]):
        group = str(header_row1.iloc[idx]).strip()
        subheader = str(header_row2.iloc[idx]).strip()
        col_id = f"col_{idx}"
        columns_meta.append(
            {
                "id": col_id,
                "group": group,
                "subheader": subheader,
                "position": idx,
            }
        )
        if not group:
            continue
        group_key = _normalize_column_name(group)
        # Exclude metadata columns from being treated as "Bucket Groups"
        if group_key in {"targetlabel", "year", "targetlevellabel", "targetlevel", "level", "tgtlevel"}:
            continue
        if group not in group_map:
            group_map[group] = []
            group_order.append(group)
        group_map[group].append(
            {
                "id": col_id,
                "subheader": subheader,
                "position": idx,
            }
        )

    target_label_id = None
    year_id = None
    target_level_label_id = None

    # Broader candidates for the Target Level Label column
    level_candidates = [
        "Target Level Label", "Target Level", "Target_Level", "Tgt Level", "Level", "Label"
    ]

    for column in columns_meta:
        # Find Target Label (Own/Cross)
        if target_label_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            ["Target Label", "Target Type"],
        ):
            target_label_id = column["id"]
        
        # Find Year
        if year_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            ["Year", "Model Year"],
        ):
            year_id = column["id"]

        # Find Target Level Label (Brand/Segment name)
        if target_level_label_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            level_candidates,
        ):
            target_level_label_id = column["id"]

    data_df = raw_df.iloc[2:].reset_index(drop=True).copy()
    data_df.columns = [col["id"] for col in columns_meta]
    metadata = {
        "columns": columns_meta,
        "groups": group_map,
        "group_order": group_order,
        "target_label_id": target_label_id,
        "year_id": year_id,
        "target_level_label_id": target_level_label_id, # Crucial ID
    }
    return data_df, metadata




def _compute_bucket_deltas(
    data_df: pd.DataFrame,
    metadata: dict,
    bucket_config: dict[str, dict[str, list[str]]],
    year1: str,
    year2: str,
    target_level_filter: str | None = None,
) -> list[tuple[str, float]]:
    """Compute Year2-Year1 deltas for each bucket group, optionally filtered by Target Level Label."""
    target_label_id = metadata.get("target_label_id")
    year_id = metadata.get("year_id")
    target_level_id = metadata.get("target_level_label_id")

    if not target_label_id:
        raise ValueError("The gatheredCN10 file is missing the Target Label column.")
    if not year_id:
        raise ValueError("The gatheredCN10 file is missing the Year column.")
    
    # Safety Check: If we need to filter by specific level (Brand A) but can't find the column,
    # we CANNOT proceed, or we will return the Global Total (Sum of All Brands).
    # It is better to return 0s than incorrect global data.
    if target_level_filter and not target_level_id:
        logger.warning(
            "Target Level Filter requested ('%s') but 'Target Level Label' column not found in buckets file. Returning 0s.",
            target_level_filter
        )
        # Return 0s for all configured groups
        deltas = []
        ordered_groups = [g for g in metadata.get("group_order", []) if g in bucket_config] or list(bucket_config.keys())
        for group in ordered_groups:
            config = bucket_config.get(group, {})
            target_labels = config.get("target_labels", [])
            if not target_labels:
                continue
            
            # Replicate the structure of the output list but with 0.0 values
            normalized_targets = []
            for label in target_labels:
                normalized = _normalize_text_value(label)
                if normalized and normalized not in normalized_targets:
                    normalized_targets.append(normalized)
            
            # Add Own/Cross/Specifics
            if "own" in normalized_targets: deltas.append((f"{DISPLAY_LABEL['Own']} {group}", 0.0))
            if "cross" in normalized_targets: deltas.append((f"{DISPLAY_LABEL['Cross']} {group}", 0.0))
            for label in target_labels:
                norm = _normalize_text_value(label)
                if norm not in {"own", "cross"}:
                    deltas.append((f"{label} {group}", 0.0))
        return deltas

    normalized_year1 = _normalize_text_value(year1)
    normalized_year2 = _normalize_text_value(year2)

    target_series = data_df[target_label_id].map(_normalize_text_value)
    year_series = data_df[year_id].map(_normalize_text_value)

    # Apply Target Level Filter if provided
    level_mask = None
    if target_level_filter and target_level_id:
        level_series = data_df[target_level_id].map(_normalize_text_value)
        normalized_filter = _normalize_text_value(target_level_filter)
        level_mask = level_series == normalized_filter

    deltas: list[tuple[str, float]] = []
    group_order = metadata.get("group_order", [])
    ordered_groups = [group for group in group_order if group in bucket_config]
    if not ordered_groups:
        ordered_groups = list(bucket_config.keys())

    for group in ordered_groups:
        config = bucket_config.get(group, {})
        selected_cols = [
            col for col in config.get("subheaders_included", []) if col in data_df.columns
        ]
        target_labels = config.get("target_labels")
        if target_labels is None:
            target_labels = []
        if not target_labels:
            continue
        ordered_targets = []
        normalized_targets = []
        for label in target_labels:
            normalized = _normalize_text_value(label)
            if normalized and normalized not in normalized_targets:
                normalized_targets.append(normalized)
                ordered_targets.append((label, normalized))
        target_label_sequence = []
        if "own" in normalized_targets:
            target_label_sequence.append(("Own", "own"))
        if "cross" in normalized_targets:
            target_label_sequence.append(("Cross", "cross"))
        for label, normalized in ordered_targets:
            if normalized not in {"own", "cross"}:
                target_label_sequence.append((label, normalized))
        if not target_label_sequence:
            deltas.append((group, 0.0))
            continue
        if not selected_cols:
            for label, _ in target_label_sequence:
                display_label = DISPLAY_LABEL.get(label, label)
                deltas.append((f"{display_label} {group}", 0.0))
            continue
        values_df = data_df[selected_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        
        # Apply masks
        year1_mask = year_series == normalized_year1
        year2_mask = year_series == normalized_year2
        
        # Merge level mask if exists
        if level_mask is not None:
            year1_mask = year1_mask & level_mask
            year2_mask = year2_mask & level_mask

        for label, normalized in target_label_sequence:
            target_mask = target_series == normalized
            year1_sum = values_df[year1_mask & target_mask].sum().sum()
            year2_sum = values_df[year2_mask & target_mask].sum().sum()
            display_label = DISPLAY_LABEL.get(label, label)
            deltas.append((f"{display_label} {group}", float(year2_sum - year1_sum)))
    return deltas




def populate_category_waterfall(
    prs,
    gathered_df: pd.DataFrame,
    parsed_df: pd.DataFrame | None = None,
    metadata: dict | None = None,
    scope_df: pd.DataFrame | None = None,
    target_labels: list[str] | None = None,
    bucket_config: dict | None = None,
    bucket_years: dict | None = None,
    modelled_in_value: str | None = None,
    metric_value: str | None = None,
):
    year1 = bucket_years.get("year1") if bucket_years else None
    year2 = bucket_years.get("year2") if bucket_years else None

    labels = _normalize_target_level_labels(target_labels)
    if not labels:
        labels = _target_level_labels_from_gathered_df_with_filters(
            gathered_df,
            year1=year1,
            year2=year2,
            target_labels=bucket_years.get("target_labels") if bucket_years else None,
        )
    if not labels:
        return

    available_slides = _available_waterfall_template_slides(prs)
    available_count = len(available_slides)
    if available_count == 0:
        raise ValueError("Could not find the <Waterfall Template> slide in the template.")
    if len(labels) > available_count:
        raise ValueError(
            "Need {needed} waterfall slides but only found {available}.".format(
                needed=len(labels),
                available=available_count,
            )
        )

    seen_partnames: set[str] = set()
    for idx, label in enumerate(labels):
        marker_text, slide = available_slides[idx]
        _ensure_unique_chart_parts_on_slide(slide, seen_partnames)
        
        # Compute bucket deltas specifically for this label
        current_bucket_data = None
        if parsed_df is not None and metadata and bucket_config and year1 and year2:
            try:
                deltas = _compute_bucket_deltas(
                    parsed_df, 
                    metadata, 
                    bucket_config, 
                    year1, 
                    year2, 
                    target_level_filter=label
                )
                bucket_labels = [group for group, _ in deltas]
                bucket_values = [value for _, value in deltas]
                current_bucket_data = {
                    "labels": bucket_labels,
                    "values": bucket_values,
                    "year1": year1,
                    "year2": year2
                }
            except Exception as e:
                logger.warning(f"Failed to compute specific buckets for {label}: {e}")

        _update_waterfall_axis_placeholders(
            prs,
            slide,
            target_level_label_value=label,
            modelled_in_value=modelled_in_value,
            metric_value=metric_value,
        )
        
        _update_waterfall_chart(slide, scope_df, gathered_df, label, current_bucket_data)
        _set_waterfall_slide_header(slide, label, marker_text=marker_text)
