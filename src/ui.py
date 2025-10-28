# src/ui.py

import streamlit as st
import plotly.express as px
from src.config import (
    SCHEMA_OPTIONS,
    DEFAULT_SCHEMA,
    TIME_WINDOW_OPTIONS,
    DEFAULT_TIME_WINDOW,
    COLUMN_RENAME_MAP,
)
from src.data import load_user_data

def render_header():
    """Renders the main title and introduction of the dashboard."""
    st.title("📊 Data Product Usage Dashboard")
    st.write("""
        This dashboard shows query usage for data products in the `INTERNE` and `EKSTERNE` schemas.
    """)

def render_filters():
    """Renders the schema and time window selection filters."""
    st.header("1. Select Filters")
    col1, col2 = st.columns([1, 2])
    with col1:
        schema_choice = st.radio(
            "**Select Schema(s)**",
            SCHEMA_OPTIONS,
            index=SCHEMA_OPTIONS.index(DEFAULT_SCHEMA)
        )
    with col2:
        selected_time_window = st.radio(
            "**Select Time Window**",
            options=list(TIME_WINDOW_OPTIONS.keys()),
            index=list(TIME_WINDOW_OPTIONS.keys()).index(DEFAULT_TIME_WINDOW)
        )
    selected_days = [TIME_WINDOW_OPTIONS[selected_time_window]]
    return schema_choice, selected_days

def render_data_filter(full_data):
    """Renders the text input for filtering data products."""
    st.header("2. Filter Data Products")
    filter_text = st.text_input(
        "Filter by object name (supports regex, e.g., `.*USAGE.*`):",
        value="."
    )
    try:
        return full_data[full_data['TABLE_NAME'].str.contains(filter_text, regex=True, case=False, na=False)]
    except Exception as e:
        st.error(f"Invalid Regex: {e}. Please correct the filter.")
        return full_data

def render_multi_version_filter():
    """Renders the checkbox for filtering multi-version data products."""
    st.header("3. Usage Overview")
    show_multi_version = st.checkbox("Show only data products with multiple versions")
    return show_multi_version

def render_overview(filtered_data, selected_days, schema_choice):
    """Renders the 'Usage Overview' section including the graph and metric."""
    col1, col2 = st.columns(2)
    with col1:
        render_usage_graph(filtered_data, selected_days, schema_choice)
    with col2:
        render_usage_change(filtered_data, selected_days)

def render_usage_graph(filtered_data, selected_days, schema_choice):
    """Renders the top 12 usage bar chart."""
    st.subheader("Top 12 Usage")
    if not selected_days:
        st.info("Please select a time window above to view the usage graph.")
        return
    if filtered_data.empty:
        st.info("No data to display in graph.")
        return

    value_vars = [f"QUERIES_LAST_{day}_DAYS" for day in selected_days]
    sort_col = f"QUERIES_LAST_{max(selected_days)}_DAYS"

    graph_data = filtered_data.sort_values(by=sort_col, ascending=False).head(12)

    if graph_data[sort_col].sum() == 0:
        st.info("No query usage to display in the graph for the selected filter.")
        return

    melted_data = graph_data.melt(
        id_vars=['FULL_OBJECT_NAME', 'TABLE_NAME', 'SCHEMA_NAME'],
        value_vars=value_vars,
        var_name='Time Period',
        value_name='Query Count'
    )

    day_map = {f"QUERIES_LAST_{day}_DAYS": f"Last {day} Days" for day in selected_days}
    melted_data['Time Period'] = melted_data['Time Period'].map(day_map)

    facet_col_arg = "Time Period" if len(selected_days) > 1 else None
    title_text = 'Top 12 Used Data Products by Time Period' if len(selected_days) > 1 else f'Top 12 Used Data Products (by {selected_days[0]}-day usage)'

    fig = px.bar(
        melted_data, x="TABLE_NAME", y="Query Count", color="SCHEMA_NAME",
        facet_col=facet_col_arg, barmode="group",
        hover_data={"FULL_OBJECT_NAME": True, "SCHEMA_NAME": True, "Time Period": True, "Query Count": True}
    )

    fig.update_layout(
        title=title_text, showlegend=(schema_choice == "Both"), yaxis_title="Number of Queries",
        legend_title_text='Schema', xaxis_title=None
    )
    fig.update_xaxes(tickangle=25, tickfont=dict(size=10))
    fig.update_yaxes(title_text="")
    if facet_col_arg:
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1], textangle=0))

    st.plotly_chart(fig, use_container_width=True)

def render_usage_change(filtered_data, selected_days):
    """Renders the metric showing usage change."""
    st.subheader("Usage Change")
    if not selected_days or filtered_data.empty:
        st.info("No data to display.")
        return

    days = selected_days[0]
    current_period_col = f"QUERIES_LAST_{days}_DAYS"
    previous_period_col = f"QUERIES_PREVIOUS_{days}_DAYS"

    selection_state = st.session_state.get("view_selection", {})
    selected_rows = selection_state.get("selection", {}).get("rows", [])

    if selected_rows:
        selected_row_data = filtered_data.iloc[selected_rows[0]]
        total_current_usage = selected_row_data[current_period_col]
        total_previous_usage = selected_row_data[previous_period_col]
        label_text = f"Change for {selected_row_data['TABLE_NAME']}"
    else:
        total_current_usage = filtered_data[current_period_col].sum()
        total_previous_usage = filtered_data[previous_period_col].sum()
        label_text = f"Change from Previous {days} Days"

    delta_text = "No change"
    if total_previous_usage > 0:
        percentage_change = ((total_current_usage - total_previous_usage) / total_previous_usage) * 100
        delta_text = f"{percentage_change:.2f}% {'increase' if percentage_change > 0 else 'decrease'}"
    elif total_current_usage > 0:
        delta_text = "Increase"
    if total_current_usage == 0 and total_previous_usage > 0:
        delta_text = "100% decrease"

    st.metric(
        label=label_text,
        value=f"{int(total_current_usage)} queries",
        delta=delta_text,
    )

def render_data_table(filtered_data, selected_days):
    """Renders the main data table of all data products."""
    st.header("4. All Data Products (Select a row to see users)")

    column_map = COLUMN_RENAME_MAP.copy()
    display_columns = list(column_map.values())

    for day in selected_days:
        col_name_db = f"QUERIES_LAST_{day}_DAYS"
        col_name_display = f"Queries (Last {day}d)"
        column_map[col_name_db] = col_name_display
        display_columns.append(col_name_display)

    display_df = filtered_data.rename(columns=column_map)

    st.dataframe(
        display_df[display_columns], key="view_selection", on_select="rerun",
        selection_mode="single-row", use_container_width=True, hide_index=True
    )

def render_user_details(filtered_data):
    """Renders the user details table for a selected data product."""
    selection_state = st.session_state.get("view_selection", {})
    selected_rows = selection_state.get("selection", {}).get("rows", [])

    if selected_rows:
        selected_index = selected_rows[0]
        selected_row = filtered_data.iloc[selected_index]
        selected_view_name = selected_row["FULL_OBJECT_NAME"]
        selected_table_name = selected_row["TABLE_NAME"]

        st.header(f"5. User Details for {selected_table_name}")

        user_data = load_user_data(selected_view_name)

        if user_data.empty:
            st.info(f"No users found for {selected_table_name} in the last 30 days.")
        else:
            st.dataframe(user_data, use_container_width=True, hide_index=True)
