import streamlit as st
import pandas as pd
import plotly.express as px # Import Plotly
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

# --- Page Configuration ---
st.set_page_config(
    page_title="Data Product Usage Dashboard",
    page_icon="📊",
    layout="wide"
)

# --- Data Loading Function (Cached) ---

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_usage_data(selected_days_tuple, schema_choice):
    """
    Connects to Snowflake, queries all data products based on schema_choice,
    and dynamically joins them with query usage history.
    """
    session = get_active_session()
    
    selected_days = list(selected_days_tuple)
    db_name = "DATAPRODUKTER"
    
    # --- 1. Dynamic Schema Filter ---
    if schema_choice == "Both":
        schema_filter_sql = "WHERE TABLE_SCHEMA IN ('INTERNE', 'EKSTERNE')"
    else:
        schema_filter_sql = f"WHERE TABLE_SCHEMA = '{schema_choice}'"

    # --- 2. CTE to get all Views (REMOVED TABLES) ---
    all_datasets_cte = f"""
    WITH all_datasets AS (
        -- Query ONLY from INFORMATION_SCHEMA.VIEWS to avoid duplicates
        (SELECT 
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) AS FULL_OBJECT_NAME
        FROM {db_name}.INFORMATION_SCHEMA.VIEWS
        {schema_filter_sql}
        )
    )
    """

    # --- 3. Dynamically build the usage counts CTE and final SELECT ---
    if not selected_days:
        # If no time windows are selected
        final_sql = f"""
        {all_datasets_cte}
        SELECT
            d.FULL_OBJECT_NAME,
            d.SCHEMA_NAME,
            d.TABLE_NAME
        FROM all_datasets d
        ORDER BY d.FULL_OBJECT_NAME
        """
    else:
        # If time windows are selected
        max_days = max(selected_days)
        
        usage_select_expressions = []
        for days in selected_days:
            usage_select_expressions.append(f"""
            COUNT(DISTINCT CASE 
                WHEN QUERY_START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) THEN QUERY_ID 
                ELSE NULL 
            END) AS QUERIES_LAST_{days}_DAYS
            """)
            # Add expression for the previous week
            usage_select_expressions.append(f"""
            COUNT(DISTINCT CASE
                WHEN QUERY_START_TIME >= DATEADD(day, -{days}*2, CURRENT_TIMESTAMP()) AND QUERY_START_TIME < DATEADD(day, -{days}, CURRENT_TIMESTAMP()) THEN QUERY_ID
                ELSE NULL
            END) AS QUERIES_PREVIOUS_{days}_DAYS
            """)
        usage_select_clause = ",\n".join(usage_select_expressions)
        
        usage_counts_cte = f"""
        , usage_counts AS (
            SELECT
                ACCESSED_OBJECT_NAME,
                {usage_select_clause}
            FROM DATAPRODUKTER.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1
            WHERE QUERY_START_TIME >= DATEADD(day, -{max_days}, CURRENT_TIMESTAMP())
            GROUP BY ACCESSED_OBJECT_NAME
        )
        """
        
        final_select_expressions = []
        for days in selected_days:
            final_select_expressions.append(f"""
            COALESCE(u.QUERIES_LAST_{days}_DAYS, 0) AS QUERIES_LAST_{days}_DAYS
            """)
            final_select_expressions.append(f"""
            COALESCE(u.QUERIES_PREVIOUS_{days}_DAYS, 0) AS QUERIES_PREVIOUS_{days}_DAYS
            """)
        
        final_select_clause = ",\n" + ",\n".join(final_select_expressions) if final_select_expressions else ""
        sort_col = f"QUERIES_LAST_{max_days}_DAYS"

        # Final query (removed OBJECT_TYPE)
        final_sql = f"""
        {all_datasets_cte}
        {usage_counts_cte}
        SELECT
            d.FULL_OBJECT_NAME,
            d.SCHEMA_NAME,
            d.TABLE_NAME
            {final_select_clause}
        FROM all_datasets d
        LEFT JOIN usage_counts u 
            ON d.FULL_OBJECT_NAME = u.ACCESSED_OBJECT_NAME
        ORDER BY {sort_col} DESC
        """
    
    pd_df = session.sql(final_sql).to_pandas()
    return pd_df

# --- NEW: Data Loading Function for User Details Table ---
@st.cache_data(ttl=3600)
def load_user_data(accessed_object_name):
    """
    Fetches the query counts per user for a single, selected data product
    for the last 7 and 30 days.
    """
    session = get_active_session()
    
    sql_query = f"""
    SELECT
        USER_NAME,
        COUNT(DISTINCT CASE 
            WHEN QUERY_START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP()) THEN QUERY_ID 
            ELSE NULL 
        END) AS QUERIES_LAST_7_DAYS,
        
        COUNT(DISTINCT CASE 
            WHEN QUERY_START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP()) THEN QUERY_ID 
            ELSE NULL 
        END) AS QUERIES_LAST_30_DAYS
        
    FROM DATAPRODUKTER.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1
    WHERE 
        ACCESSED_OBJECT_NAME = '{accessed_object_name}'
        AND QUERY_START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP()) -- Filter for max range
    GROUP BY 
        USER_NAME
    ORDER BY
        QUERIES_LAST_30_DAYS DESC
    """
    
    pd_df = session.sql(sql_query).to_pandas()
    # Rename columns for the final table
    pd_df = pd_df.rename(columns={
        "USER_NAME": "User Name",
        "QUERIES_LAST_7_DAYS": "Queries (Last 7d)",
        "QUERIES_LAST_30_DAYS": "Queries (Last 30d)"
    })
    return pd_df

# --- Main Application ---

st.title("📊 Data Product Usage Dashboard")
st.write("""
    This dashboard shows query usage for data products in the `INTERNE` and `EKSTERNE` schemas.
""")

# --- 1. Selection Boxes ---
st.header("1. Select Filters")
col1, col2 = st.columns([1, 2]) # 1/3 width for radio, 2/3 for checkboxes

with col1:
    schema_choice = st.radio(
        "**Select Schema(s)**",
        ["Both", "INTERNE", "EKSTERNE"],
        index=0  # Default to "Both"
    )

with col2:
    time_window_options = {
        "Last 7 Days": 7,
        "Last 14 Days": 14,
        "Last 30 Days": 30,
        "Last 90 Days": 90,
    }
    selected_time_window = st.radio(
        "**Select Time Window**",
        options=time_window_options.keys(),
        index=3  # Default to "Last 90 Days"
    )
    selected_days = [time_window_options[selected_time_window]]

try:
    # Load the data, passing both filter options to the cache
    full_data = load_usage_data(tuple(selected_days), schema_choice)

    if full_data.empty:
        st.warning(f"No data products found for schema filter: '{schema_choice}'.")
    else:
        # --- 2. Filter Query Box ---
        st.header("2. Filter Data Products")
        filter_text = st.text_input(
            "Filter by object name (supports regex, e.g., `.*USAGE.*`):", 
            value=".*"
        )

        try:
            # Filter on the FULL_OBJECT_NAME which contains the full path
            filtered_data = full_data[full_data['FULL_OBJECT_NAME'].str.contains(filter_text, regex=True, case=False, na=False)]
        except Exception as e:
            st.error(f"Invalid Regex: {e}. Please correct the filter.")
            filtered_data = full_data
        
        if filtered_data.empty and not full_data.empty:
            st.info("Your filter returned no results.")

        # --- 3. Usage Graph (with Plotly) ---
        st.header("3. Usage Overview")
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top 10 Usage")
            if not selected_days:
                st.info("Please select a time window above to view the usage graph.")
            elif filtered_data.empty:
                st.info("No data to display in graph.")
            else:
                value_vars = [f"QUERIES_LAST_{day}_DAYS" for day in selected_days]
                sort_col = f"QUERIES_LAST_{max(selected_days)}_DAYS"
                
                graph_data = filtered_data.sort_values(by=sort_col, ascending=False).head(10)
                
                if graph_data[sort_col].sum() == 0:
                    st.info("No query usage to display in the graph for the selected filter.")
                else:
                    # "Melt" the dataframe
                    melted_data = graph_data.melt(
                        id_vars=['FULL_OBJECT_NAME', 'TABLE_NAME', 'SCHEMA_NAME'],
                        value_vars=value_vars,
                        var_name='Time Period',
                        value_name='Query Count'
                    )

                    day_map = {f"QUERIES_LAST_{day}_DAYS": f"Last {day} Days" for day in selected_days}
                    melted_data['Time Period'] = melted_data['Time Period'].map(day_map)

                    if len(selected_days) == 1:
                        facet_col_arg = None
                        title_text = f'Top 10 Used Data Products (by {selected_days[0]}-day usage)'
                    else:
                        facet_col_arg = "Time Period"
                        title_text = 'Top 10 Used Data Products by Time Period'

                    # Create the bar chart
                    fig = px.bar(
                        melted_data,
                        x="TABLE_NAME",           # View names on X-axis
                        y="Query Count",
                        color="SCHEMA_NAME",      # Color code by schema
                        facet_col=facet_col_arg,  # Facet by time period
                        barmode="group",
                        hover_data={
                            "FULL_OBJECT_NAME": True,
                            "SCHEMA_NAME": True,
                            "Time Period": True,
                            "Query Count": True
                        }
                    )

                    # --- Customize Layout ---
                    show_legend = True if schema_choice == "Both" else False

                    fig.update_layout(
                        title=title_text,
                        showlegend=show_legend,
                        yaxis_title="Number of Queries",
                        legend_title_text='Schema',
                        xaxis_title=None
                    )

                    fig.update_xaxes(
                        tickangle=25,
                        tickfont=dict(size=10)
                    )

                    fig.update_yaxes(title_text="")

                    if facet_col_arg:
                        fig.for_each_annotation(
                            lambda a: a.update(
                                text=a.text.split("=")[-1],
                                textangle=0
                            )
                        )

                    st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Usage Comparison")
            if not selected_days:
                st.info("Please select a time window above to view the usage graph.")
            elif filtered_data.empty:
                st.info("No data to display in graph.")
            else:
                days = selected_days[0]
                current_period_col = f"QUERIES_LAST_{days}_DAYS"
                previous_period_col = f"QUERIES_PREVIOUS_{days}_DAYS"

                # Calculate the total usage for the current and previous periods
                total_current_usage = filtered_data[current_period_col].sum()
                total_previous_usage = filtered_data[previous_period_col].sum()

                # Calculate the percentage change
                if total_previous_usage > 0:
                    percentage_change = ((total_current_usage - total_previous_usage) / total_previous_usage) * 100
                else:
                    percentage_change = float('inf') if total_current_usage > 0 else 0

                # Display the percentage change
                if percentage_change > 0:
                    st.metric(label="Usage Change", value=f"{total_current_usage} queries", delta=f"{percentage_change:.2f}% (increase)")
                elif percentage_change < 0:
                    st.metric(label="Usage Change", value=f"{total_current_usage} queries", delta=f"{percentage_change:.2f}% (decrease)")
                else:
                    st.metric(label="Usage Change", value=f"{total_current_usage} queries", delta="No change")

                # Create the comparison graph
                comparison_data = filtered_data.nlargest(10, current_period_col)
                comparison_data = comparison_data.melt(
                    id_vars=['TABLE_NAME'],
                    value_vars=[current_period_col, previous_period_col],
                    var_name='Period',
                    value_name='Query Count'
                )
                comparison_data['Period'] = comparison_data['Period'].map({
                    current_period_col: f"Last {days} Days",
                    previous_period_col: f"Previous {days} Days"
                })

                fig_comp = px.bar(
                    comparison_data,
                    x="TABLE_NAME",
                    y="Query Count",
                    color="Period",
                    barmode="group",
                    title="Top 10 Usage Comparison"
                )
                st.plotly_chart(fig_comp, use_container_width=True)

        # --- 4. Sortable Table (MODIFIED for selection) ---
        st.header("4. All Data Products (Select a row to see users)")
        
        column_rename_map = {
            "FULL_OBJECT_NAME": "Full Object Name",
            "SCHEMA_NAME": "Schema",
            "TABLE_NAME": "Name"
        }
        display_columns = ["Full Object Name", "Schema", "Name"]
        
        for day in selected_days:
            col_name_db = f"QUERIES_LAST_{day}_DAYS"
            col_name_display = f"Queries (Last {day}d)"
            column_rename_map[col_name_db] = col_name_display
            display_columns.append(col_name_display)
        
        display_df = filtered_data.rename(columns=column_rename_map)

        st.dataframe(
            display_df[display_columns], 
            key="view_selection",           # Add a key to track selection
            on_select="rerun",              # Rerun the script when a row is clicked
            selection_mode="single-row",    # Allow only one row to be selected
            use_container_width=True,
            hide_index=True
        )

        # --- 5. NEW: User Details Table (CORRECTED) ---
        
        # Get the selection state safely
        selection_state = st.session_state.get("view_selection", {})
        selected_rows = selection_state.get("selection", {}).get("rows", [])

        # Check if a row has been selected
        if selected_rows:
            # Get the index of the selected row
            selected_index = selected_rows[0]
            
            # Get the data from the *original filtered* dataframe (filtered_data)
            selected_row = filtered_data.iloc[selected_index] 
            selected_view_name = selected_row["FULL_OBJECT_NAME"]
            selected_table_name = selected_row["TABLE_NAME"]

            st.header(f"5. User Details for {selected_table_name}")
            
            # Call the new function to get user data
            user_data = load_user_data(selected_view_name)
            
            if user_data.empty:
                st.info(f"No users found for {selected_table_name} in the last 30 days.")
            else:
                st.dataframe(user_data, use_container_width=True, hide_index=True)

except SnowparkSQLException as e:
    st.error(f"Error connecting to or querying Snowflake: {e}")
    st.error("Please ensure the view `DATAPRODUKTER.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1` exists and your role has permission to access it and the `DATAPRODUKTER.INFORMATION_SCHEMA`.")
except Exception as e:
    st.error(f"An unexpected error occurred: {e}")