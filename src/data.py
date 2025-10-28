# src/data.py

import streamlit as st
from snowflake.snowpark.context import get_active_session
from src.config import DB_NAME, USER_DETAILS_COLUMNS

@st.cache_data(ttl=3600)
def load_usage_data(selected_days_tuple, schema_choice):
    """
    Connects to Snowflake, queries all data products based on schema_choice,
    and dynamically joins them with query usage history.
    """
    session = get_active_session()

    selected_days = list(selected_days_tuple)

    if schema_choice == "Both":
        schema_filter_sql = "WHERE TABLE_SCHEMA IN ('INTERNE', 'EKSTERNE')"
    else:
        schema_filter_sql = f"WHERE TABLE_SCHEMA = '{schema_choice}'"

    all_datasets_cte = f"""
    WITH all_datasets AS (
        (SELECT
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) AS FULL_OBJECT_NAME
        FROM {DB_NAME}.INFORMATION_SCHEMA.VIEWS
        {schema_filter_sql}
        )
    )
    """

    if not selected_days:
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
        max_days = max(selected_days)

        usage_select_expressions = []
        for days in selected_days:
            usage_select_expressions.append(f"""
            COUNT(DISTINCT CASE
                WHEN QUERY_START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) THEN QUERY_ID
                ELSE NULL
            END) AS QUERIES_LAST_{days}_DAYS
            """)
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
            FROM {DB_NAME}.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1
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

    FROM {DB_NAME}.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1
    WHERE
        ACCESSED_OBJECT_NAME = '{accessed_object_name}'
        AND QUERY_START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY
        USER_NAME
    ORDER BY
        QUERIES_LAST_30_DAYS DESC
    """

    pd_df = session.sql(sql_query).to_pandas()
    pd_df = pd_df.rename(columns=USER_DETAILS_COLUMNS)
    return pd_df
