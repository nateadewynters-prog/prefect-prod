"""
Central configuration for the PBI Refresh & Data Tool.
This file handles environment variable loading and defines constants.
"""

import os
from dotenv import load_dotenv

# Initialize environment variables
load_dotenv()

# --- AZURE & AUTH CONFIG ---
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = "figures@dewynters.com"

# --- PATHS ---
# Relative to the project root
JSON_PATH = './data/PowerBI_Report_IDs.json'

# --- DATABASE CONFIG ---
DB_CONFIG = {
    'server': os.getenv("SQL_SERVER"),
    'database': 'TicketingDS',
    'username': os.getenv("SQL_USERNAME"),
    'password': os.getenv("SQL_PASSWORD"),
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# --- AUTOMATED EMAIL CONFIG ---
SHOWS_CONFIG = [
    {
        "show_name": "The Devil Wears Prada", "show_id": 180, "db_type": "legacy",
        "pbi_workspace_id": "b5687f95-8331-4389-88bc-10680652c6f7", "pbi_report_id": "24784969-474d-4c16-bd45-88a71b8167dd",
        "dashboard_url": "https://app.powerbi.com/groups/b5687f95-8331-4389-88bc-10680652c6f7/reports/24784969-474d-4c16-bd45-88a71b8167dd",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com"]
    },
    {
        "show_name": "Beetlejuice", "show_id": 281, "db_type": "legacy",
        "pbi_workspace_id": "9fe3b075-b754-4763-983e-655771e0b7c4", "pbi_report_id": "5d44f020-82c0-46da-938a-b90c6906b079",
        "dashboard_url": "https://app.powerbi.com/groups/9fe3b075-b754-4763-983e-655771e0b7c4/reports/5d44f020-82c0-46da-938a-b90c6906b079/0920519f35b44a81ba38",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com"]
    },
    {
        "show_name": "Mamma Mia!", "show_id": 8, "db_type": "legacy",
        "pbi_workspace_id": "4900e0ac-9477-4fc1-a82c-6ddc35546023", "pbi_report_id": "00a4bb1a-0691-417e-a94b-f9d09965bf45",
        "dashboard_url": "https://app.powerbi.com/groups/4900e0ac-9477-4fc1-a82c-6ddc35546023/reports/00a4bb1a-0691-417e-a94b-f9d09965bf45/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "show_name": "Moulin Rouge!", "show_id": 45, "db_type": "legacy",
        "pbi_workspace_id": "d8e48a79-0972-4f4e-a6da-891f284f7953", "pbi_report_id": "a389ea5b-949f-4bb7-b4f2-97571dee86b3",
        "dashboard_url": "https://app.powerbi.com/groups/d8e48a79-0972-4f4e-a6da-891f284f7953/reports/a389ea5b-949f-4bb7-b4f2-97571dee86b3/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    }
]
