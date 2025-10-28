# steamlit_app.py

import streamlit as st
from snowflake.snowpark.exceptions import SnowparkSQLException
from src.config import PAGE_CONFIG
from src.data import load_usage_data
from src.ui import (
    render_header,
    render_filters,
    render_data_filter,
    render_overview,
    render_data_table,
    render_user_details,
)

def main():
    """Main function to run the Streamlit application."""
    st.set_page_config(**PAGE_CONFIG)

    render_header()

    schema_choice, selected_days = render_filters()

    try:
        full_data = load_usage_data(tuple(selected_days), schema_choice)

        if full_data.empty:
            st.warning(f"No data products found for schema filter: '{schema_choice}'.")
            return

        filtered_data = render_data_filter(full_data)

        if filtered_data.empty and not full_data.empty:
            st.info("Your filter returned no results.")

        render_overview(filtered_data, selected_days, schema_choice)
        render_data_table(filtered_data, selected_days)
        render_user_details(filtered_data)

    except SnowparkSQLException as e:
        st.error(f"Error connecting to or querying Snowflake: {e}")
        st.error("Please ensure the view `DATAPRODUKTER.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1` exists and your role has permission to access it and the `DATAPRODUKTER.INFORMATION_SCHEMA`.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
