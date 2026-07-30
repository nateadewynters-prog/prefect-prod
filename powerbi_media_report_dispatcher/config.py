"""
config.py — every setting in one place.

This is the ONLY file you edit regularly: to add a show, copy an entry in
SHOWS_CONFIG and fill in its Power BI IDs and recipients. Everything else
(secrets, paths) comes from the shared .env / environment, so nothing
sensitive is hard-coded.

Import order matters a little: load_dotenv() runs here, at import time, so
any module that does `from config import ...` gets env vars that are already
loaded. That's why the other modules read their settings from here instead
of calling os.getenv themselves.
"""

import os

from dotenv import load_dotenv

load_dotenv()  # in the container this reads /app/.env (mounted read-only)

# ---------------------------------------------------------------------------
# SECRETS  — everything comes from the shared .env / environment.
# Nothing sensitive is hard-coded.
# ---------------------------------------------------------------------------
TENANT_ID = os.getenv("AZURE_TENANT_ID") or os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID") or os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")
SENDER_EMAIL = os.getenv("BUSINESS_INTELLIGENCE_INBOX_ADDRESS", "figures@dewynters.com")

# BigQuery. GOOGLE_APPLICATION_CREDENTIALS must point at the mounted
# service-account JSON (set in docker-compose). We never hard-code a laptop path.
PROJECT_ID = os.getenv("GBQ_PROJECT_ID", "dewynters")
GBQ_TABLE = ("dewynters.GBQ_Dewynters_MAD_Dashboard_All_Shows"
             ".GBQ_Dewynters_MAD_Dashboard_All_Shows")

# Where the shared SQLite state lives (locks / logs / history). In Docker this
# is set to /app/data/media_dispatcher_state.db — it must stay distinct from
# the sales dispatcher's DB (different schema, and shared locks would make the
# two tools block each other).
DB_PATH = os.getenv("DB_PATH", "dispatcher_state.db")

# ---------------------------------------------------------------------------
# CONFIGURATION  — one entry per show.
# ---------------------------------------------------------------------------
BASE_RECIPIENTS = [
    "a.cameron@dewynters.com", "f.joseph@dewynters.com", "c.dobson@dewynters.com",
    "h.brown@dewynters.com", "s.netherton@dewynters.com", "b.hurring@dewynters.com",
    "a.kirkham@dewynters.com", "a.conran@dewynters.com",
]

SHOWS_CONFIG = [
    {
        "id": "1", "code": "MNT", "show_name": "My Neighbour Totoro",
        "gbq_name": "My Neighbour Totoro",
        "workspace_id": "2d12753e-740c-421c-b84c-20790dedc4f2",
        "report_id": "41bfcfec-dba2-490b-9303-379fe1ed4d4c",
        "dashboard_url": "https://app.powerbi.com/groups/2d12753e-740c-421c-b84c-20790dedc4f2/reports/41bfcfec-dba2-490b-9303-379fe1ed4d4c/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS,
    },
    {
        "id": "2", "code": "BJ", "show_name": "Beetlejuice",
        "gbq_name": "Beetlejuice",
        "workspace_id": "9fe3b075-b754-4763-983e-655771e0b7c4",
        "report_id": "66d7367e-3ef8-4752-8df3-58fb40da20bd",
        "dashboard_url": "https://app.powerbi.com/groups/9fe3b075-b754-4763-983e-655771e0b7c4/reports/66d7367e-3ef8-4752-8df3-58fb40da20bd/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS + ["l.thorpe@dewynters.com", "b.jefferis@dewynters.com"],
    },
    {
        "id": "3", "code": "DWP", "show_name": "The Devil Wears Prada",
        "gbq_name": "The Devil Wears Prada",
        "workspace_id": "b5687f95-8331-4389-88bc-10680652c6f7",
        "report_id": "bfefb4a8-8162-4d48-8bf7-2ba42af332c6",
        "dashboard_url": "https://app.powerbi.com/groups/b5687f95-8331-4389-88bc-10680652c6f7/reports/bfefb4a8-8162-4d48-8bf7-2ba42af332c6/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS + ["c.wentworth@dewynters.com"],
    },
    {
        "id": "4", "code": "FRA", "show_name": "Frameless",
        "gbq_name": "Frameless",
        "workspace_id": "26b77406-bb46-4a03-a8bd-416757804e59",
        "report_id": "03e8cf8c-2cbf-4aaf-bb74-0fc73687307a",
        "dashboard_url": "https://app.powerbi.com/groups/26b77406-bb46-4a03-a8bd-416757804e59/reports/03e8cf8c-2cbf-4aaf-bb74-0fc73687307a/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS + ["f.carpenter@dewynters.com"],
    },
    {
        "id": "5", "code": "MML", "show_name": "Magic Mike Live",
        "gbq_name": "Magic Mike Live",
        "workspace_id": "67ad38b6-3981-401a-9032-2d0807b5f8d6",
        "report_id": "051c3e87-5bdf-4dda-8885-7103201d9a67",
        "dashboard_url": "https://app.powerbi.com/groups/67ad38b6-3981-401a-9032-2d0807b5f8d6/reports/051c3e87-5bdf-4dda-8885-7103201d9a67/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS + ["k.eastham@dewynters.com", "f.carpenter@dewynters.com"],
    },
]

# The fixed pipeline. The `id` values must match the `stage` tags used in
# pipeline.py and the STAGES list in dispatcher.html.
STAGES = ["auth", "bigquery", "export_pptx", "export_png", "email"]


def get_config(show_id: str) -> dict | None:
    return next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)


def is_configured(config: dict) -> bool:
    """A show can only be dispatched once its Power BI IDs are filled in."""
    return bool(config["workspace_id"] and config["report_id"])
