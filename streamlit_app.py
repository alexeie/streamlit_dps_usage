# steamlit_app.py

import streamlit as st
from snowflake.snowpark.exceptions import SnowparkSQLException
from src.config import PAGE_CONFIG
from src.data import load_usage_data
from src.ui import (
    render_header,
    render_filters,
    render_data_filter,
    render_multi_version_filter,
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

        show_multi_version = render_multi_version_filter()

        if show_multi_version:
            base_names = filtered_data['TABLE_NAME'].str.replace(r'_V\d+$', '', regex=True, case=False)
            name_counts = base_names.value_counts()
            multi_version_names = name_counts[name_counts > 1].index

            # Filter the data to only include the multi-version names
            multi_version_mask = base_names.isin(multi_version_names)
            final_data = filtered_data[multi_version_mask]
        else:
            final_data = filtered_data

        render_overview(final_data, selected_days, schema_choice)
        render_data_table(final_data, selected_days)
        render_user_details(final_data)

    except SnowparkSQLException as e:
        st.error(f"Error connecting to or querying Snowflake: {e}")
        st.error("Please ensure the view `DATAPRODUKTER.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1` exists and your role has permission to access it and the `DATAPRODUKTER.INFORMATION_SCHEMA`.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
