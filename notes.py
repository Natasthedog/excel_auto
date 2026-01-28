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
        if group_key in {"targetlabel", "year", "targetlevellabel", "targetlevel"}:
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
    target_level_label_id = None  # NEW

    for column in columns_meta:
        if target_label_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            ["Target Label"],
        ):
            target_label_id = column["id"]
        if year_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            ["Year"],
        ):
            year_id = column["id"]
        # NEW: Find Target Level Label ID
        if target_level_label_id is None and _two_row_column_match(
            column["group"],
            column["subheader"],
            ["Target Level Label", "Target Level"],
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
        "target_level_label_id": target_level_label_id, # NEW
    }
    return data_df, metadata

def _compute_bucket_deltas(
    data_df: pd.DataFrame,
    metadata: dict,
    bucket_config: dict[str, dict[str, list[str]]],
    year1: str,
    year2: str,
    target_level_filter: str | None = None, # NEW argument
) -> list[tuple[str, float]]:
    """Compute Year2-Year1 deltas for each bucket group, optionally filtered by Target Level Label."""
    target_label_id = metadata.get("target_label_id")
    year_id = metadata.get("year_id")
    target_level_id = metadata.get("target_level_label_id")

    if not target_label_id:
        raise ValueError("The gatheredCN10 file is missing the Target Label column.")
    if not year_id:
        raise ValueError("The gatheredCN10 file is missing the Year column.")

    normalized_year1 = _normalize_text_value(year1)
    normalized_year2 = _normalize_text_value(year2)

    target_series = data_df[target_label_id].map(_normalize_text_value)
    year_series = data_df[year_id].map(_normalize_text_value)

    # NEW: Apply Target Level Filter if provided
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
    parsed_df: pd.DataFrame | None = None, # NEW
    metadata: dict | None = None, # NEW
    scope_df: pd.DataFrame | None = None,
    target_labels: list[str] | None = None,
    bucket_config: dict | None = None, # NEW
    bucket_years: dict | None = None, # NEW
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
            target_labels=bucket_years.get("target_labels") if bucket_years else None, # Reuse stored target filters if available
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
        
        # NEW: Compute bucket deltas specifically for this label
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
        
        # Pass the calculated specific data
        _update_waterfall_chart(slide, scope_df, gathered_df, label, current_bucket_data)
        _set_waterfall_slide_header(slide, label, marker_text=marker_text)





def build_pptx_from_template(
    template_bytes,
    df,
    target_brand=None,
    project_name=None,
    scope_df=None,
    product_description_df=None,
    waterfall_targets=None,
    # New Arguments
    parsed_df=None,
    metadata=None,
    bucket_config=None,
    bucket_years=None,
    # End New Arguments
    modelled_in_value: str | None = None,
    metric_value: str | None = None,
):
    # ... existing Slide 1 & 2 logic ...
    
    if project_name == "MMx":
        try:
            populate_category_waterfall(
                prs,
                df,
                parsed_df=parsed_df,
                metadata=metadata,
                scope_df=scope_df,
                target_labels=waterfall_targets,
                bucket_config=bucket_config,
                bucket_years=bucket_years,
                modelled_in_value=modelled_in_value,
                metric_value=metric_value,
            )
        except Exception:
            logger.exception("Failed to populate category waterfall slides.")
            raise

    # ... save and return ...







@callback(
    Output("download","data"),
    Output("status","children"),
    Input("go","n_clicks"),
    State("data-upload","contents"),
    State("data-upload","filename"),
    State("scope-upload", "contents"),
    State("scope-upload", "filename"),
    State("project-select", "value"),
    State("waterfall-targets", "value"),
    # New States needed
    State("bucket-config", "data"),
    State("bucket-year1", "value"),
    State("bucket-year2", "value"),
    prevent_initial_call=True
)
def generate_deck(
    n_clicks,
    data_contents,
    data_name,
    scope_contents,
    scope_name,
    project_name,
    waterfall_targets,
    bucket_config,
    year1,
    year2,
):
    if not data_contents or not project_name:
        return no_update, "Please upload the data file and select a project."

    template_path = PROJECT_TEMPLATES.get(project_name)
    if not template_path or not template_path.exists():
        return no_update, "The selected project template could not be found."
    try:
        # 1. Standard DF for KPI tables/Scope
        df = df_from_contents(data_contents, data_name)
        
        # 2. Parsed DF for Waterfall Buckets (Robust 2-row header parsing)
        parsed_df = None
        metadata = None
        if bucket_config and year1 and year2:
            try:
                raw_df = raw_df_from_contents(data_contents, data_name)
                parsed_df, metadata = _parse_two_row_header_dataframe(raw_df)
            except Exception as e:
                logger.warning(f"Could not parse gatheredCN10 for buckets: {e}")

        # Bucket Years Metadata
        bucket_years = {"year1": year1, "year2": year2} if year1 and year2 else None
        
        scope_df = None
        product_description_df = None
        project_details_df = None
        modelled_in_value = None
        metric_value = None
        if scope_contents:
            try:
                scope_df = scope_df_from_contents(scope_contents, scope_name)
            except Exception:
                scope_df = None
            try:
                product_description_df = product_description_df_from_contents(
                    scope_contents, scope_name
                )
            except Exception:
                product_description_df = None
            try:
                project_details_df = project_details_df_from_contents(
                    scope_contents, scope_name
                )
            except Exception:
                project_details_df = None
        if project_details_df is not None:
            modelled_in_value = _project_detail_value_from_df(
                project_details_df,
                "modelled in",
                [
                    "Sales will be modelled in",
                    "Sales will be modeled in",
                    "Sales modelled in",
                    "Sales modeled in",
                ],
                "Sales will be modelled in",
            )
            metric_value = _project_detail_value_from_df(
                project_details_df,
                "metric",
                [
                    "Volume metric (unique per dataset)",
                    "Volume metric unique per dataset",
                    "Volume metric",
                ],
                "Volume metric (unique per dataset)",
            )
        target_brand = target_brand_from_scope_df(scope_df)
        template_bytes = template_path.read_bytes()

        pptx_bytes = build_pptx_from_template(
            template_bytes,
            df,
            target_brand,
            project_name,
            scope_df,
            product_description_df,
            waterfall_targets,
            parsed_df=parsed_df,        # Pass new
            metadata=metadata,          # Pass new
            bucket_config=bucket_config,# Pass new
            bucket_years=bucket_years,  # Pass new
            modelled_in_value=modelled_in_value,
            metric_value=metric_value,
        )
        return dcc.send_bytes(lambda buff: buff.write(pptx_bytes), "deck.pptx"), "Building deck..."

    except Exception as exc:
        logger.exception("Deck generation failed.")
        message = str(exc).strip()
        if not message:
            message = "Unknown error. Check server logs for details."
        return no_update, f"Error ({type(exc).__name__}): {message}"
