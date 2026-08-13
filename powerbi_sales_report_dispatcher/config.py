"""
config.py — settings and the show list.

load_dotenv() runs here, at import time, before any constant below is read.
That means any module doing `from config import ...` gets env vars that are
already loaded, which is why the other modules read their settings from here
instead of calling os.getenv themselves.

SHOWS_CONFIG is the part you edit regularly — add a show, change a recipient
list, point a show at a different Power BI report.
"""

import os

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# SECRETS  — everything comes from the shared .env / environment.
# ---------------------------------------------------------------------------
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = os.getenv("BUSINESS_INTELLIGENCE_INBOX_ADDRESS")

# Where the SQLite state lives. Compose sets this to /app/data/dispatcher_state.db
# so the file survives a rebuild. Keep it distinct from the media dispatcher's
# media_dispatcher_state.db — the two apps have different history schemas, and
# sharing a file would also make them block each other on the global lock.
DB_PATH = os.getenv("DB_PATH", "dispatcher_state.db")

# ---------------------------------------------------------------------------
# SHOWS
# ---------------------------------------------------------------------------
SHOWS_CONFIG = [
    {
        "id": "1", "show_name": "The Devil Wears Prada", "show_id": 180, "db_type": "Legacy",
        "pbi_workspace_id": "b5687f95-8331-4389-88bc-10680652c6f7",
        "pbi_report_id": "24784969-474d-4c16-bd45-88a71b8167dd",
        "pbi_dataset_id": "3388428f-e0b7-4d23-b65d-21f77c8d111b",
        "dashboard_url": "https://app.powerbi.com/groups/b5687f95-8331-4389-88bc-10680652c6f7/reports/24784969-474d-4c16-bd45-88a71b8167dd",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "2", "show_name": "Beetlejuice", "show_id": 281, "db_type": "Legacy",
        "pbi_workspace_id": "9fe3b075-b754-4763-983e-655771e0b7c4",
        "pbi_report_id": "5d44f020-82c0-46da-938a-b90c6906b079",
        "pbi_dataset_id": "fee1f648-be9b-4d16-b458-df868dee474d",
        "dashboard_url": "https://app.powerbi.com/groups/9fe3b075-b754-4763-983e-655771e0b7c4/reports/5d44f020-82c0-46da-938a-b90c6906b079/0920519f35b44a81ba38",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "3", "show_name": "Mamma Mia!", "show_id": 8, "db_type": "Legacy",
        "pbi_workspace_id": "4900e0ac-9477-4fc1-a82c-6ddc35546023",
        "pbi_report_id": "00a4bb1a-0691-417e-a94b-f9d09965bf45",
        "pbi_dataset_id": "445be91a-db44-4716-952c-69825afa9270",
        "dashboard_url": "https://app.powerbi.com/groups/4900e0ac-9477-4fc1-a82c-6ddc35546023/reports/00a4bb1a-0691-417e-a94b-f9d09965bf45/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "4", "show_name": "Moulin Rouge! The Musical", "show_id": 45, "db_type": "Legacy",
        "pbi_workspace_id": "d8e48a79-0972-4f4e-a6da-891f284f7953",
        "pbi_report_id": "a389ea5b-949f-4bb7-b4f2-97571dee86b3",
        "pbi_dataset_id": "ee878be9-5355-412d-ba52-d4c4c2661cf0",
        "dashboard_url": "https://app.powerbi.com/groups/d8e48a79-0972-4f4e-a6da-891f284f7953/reports/a389ea5b-949f-4bb7-b4f2-97571dee86b3/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "5", "show_name": "My Neighbour Totoro", "show_id": 222, "db_type": "Legacy",
        "pbi_workspace_id": "2d12753e-740c-421c-b84c-20790dedc4f2",
        "pbi_report_id": "5ba3d957-c0ba-4027-8aea-12730ede5113",
        "pbi_dataset_id": "c14312f5-bd83-44cc-95ef-27ba1b86ddbe",
        "dashboard_url": "https://app.powerbi.com/groups/2d12753e-740c-421c-b84c-20790dedc4f2/reports/5ba3d957-c0ba-4027-8aea-12730ede5113",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "6", "show_name": "Magic Mike", "show_id": 44, "db_type": "TransactLive", # Unique identifier for the new DB
        "pbi_workspace_id": "67ad38b6-3981-401a-9032-2d0807b5f8d6",
        "pbi_report_id": "c4fa1a2d-7882-4bba-91c8-b8bb1114cdb5",
        "pbi_dataset_id": "2176834f-4728-4e1a-bc23-196b43d70b2d",
        "dashboard_url": "https://app.powerbi.com/groups/67ad38b6-3981-401a-9032-2d0807b5f8d6/reports/c4fa1a2d-7882-4bba-91c8-b8bb1114cdb5",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    }
]

# The pipeline's stage tags, in the order they run. These ids must match the
# stage strings set in pipeline.py and the STAGES list in dispatcher.html —
# the front-end stepper keys off them.
STAGES = ["auth", "refresh", "sql", "export", "download", "render", "email"]


def get_config(show_id: str) -> dict | None:
    """Look up a show by the id that arrives in the URL (always a string)."""
    return next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)
