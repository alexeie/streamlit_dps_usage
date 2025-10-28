# 📊 Data Product Usage Dashboard

This Streamlit application provides a dashboard to visualize and analyze the usage of data products within a Snowflake environment. It helps data teams understand which data products are being used, by whom, and how frequently.

![Data Product Usage Dashboard](./image.png)

## Key Features

*   **Filter by Schema and Time Window**: Users can filter the data by one or more schemas (e.g., `INTERNE`, `EKSTERNE`) and select a specific time window for analysis (e.g., last 7, 14, 30, or 90 days).
*   **Filter by Data Product Name**: The dashboard allows for filtering data products by their object name using regular expressions, enabling users to quickly find specific data products.
*   **Usage Overview**: A graphical representation of the top 12 most used data products, providing a quick overview of the most popular datasets.
*   **Identify Outdated Data Products**: The "Show only data products with multiple versions" filter helps identify older versions of data products. By selecting this option, users can see if these outdated versions are still being used and by whom, which is crucial for data product lifecycle management and deprecation planning.
*   **Detailed Data Table**: A comprehensive table of all data products, showing their usage metrics. Users can select a row in this table to see more detailed information.
*   **User-Level Details**: When a data product is selected, the dashboard displays a table of all users who have queried that data product in the last 30 days, along with the number of queries they have run.

## Getting Started

### Prerequisites

*   A Snowflake account with the necessary permissions to access the `DATAPRODUKTER.INFORMATION_SCHEMA` and the `DATAPRODUKTER.INTERNE.DATA_PRODUCT_USAGE_HISTORY_V1` view.
*   Python 3.11 or higher.
*   Conda installed on your local machine.

### Installation and Running the Application

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-repo/data-product-usage-dashboard.git
    cd data-product-usage-dashboard
    ```

2.  **Create and activate the Conda environment**:
    ```bash
    conda env create -f environment.yml
    conda activate app_environment
    ```

3.  **Run the Streamlit application**:
    ```bash
    streamlit run streamlit_app.py
    ```

The application will be available at `http://localhost:8501` in your web browser.
