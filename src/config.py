# src/config.py

# --- Page Configuration ---
PAGE_CONFIG = {
    "page_title": "Data Product Usage Dashboard",
    "page_icon": "📊",
    "layout": "wide",
}

# --- Database and Schema ---
DB_NAME = "DATAPRODUKTER"
SCHEMA_OPTIONS = ["Both", "INTERNE", "EKSTERNE"]
DEFAULT_SCHEMA = "Both"

# --- Time Windows ---
TIME_WINDOW_OPTIONS = {
    "Last 7 Days": 7,
    "Last 14 Days": 14,
    "Last 30 Days": 30,
    "Last 90 Days": 90,
}
DEFAULT_TIME_WINDOW = "Last 90 Days"

# --- Column Renaming ---
COLUMN_RENAME_MAP = {
    "FULL_OBJECT_NAME": "Full Object Name",
    "SCHEMA_NAME": "Schema",
    "TABLE_NAME": "Name",
}

USER_DETAILS_COLUMNS = {
    "USER_NAME": "User Name",
    "QUERIES_LAST_7_DAYS": "Queries (Last 7d)",
    "QUERIES_LAST_30_DAYS": "Queries (Last 30d)",
}
