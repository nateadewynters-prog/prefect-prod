import os
import requests as r
from dotenv import load_dotenv
from pathlib import Path
from prefect.runtime import flow_run
from dataclasses import dataclass
from typing import Dict, Any

# --- Core Configuration ---
# Resolves to C:\Prefect\.env regardless of which script calls it
ENV_PATH = Path("/opt/prefect/prod/.env")
BASE_URL = "https://api.falcon.io"

@dataclass
class ValidationResult:
    """Standardized Data Contract for parser validation returns."""
    status: str  # Strictly "PASSED", "FAILED", or "UNVALIDATED"
    message: str
    metrics: Dict[str, Any]

def setup_environment():
    """Loads the centralized .env file from the Prefect root."""
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)
    else:
        print(f"⚠️ Warning: .env not found at {ENV_PATH}")

def get_db_connection():
    """Returns an active pyodbc connection using centralized env vars."""
    import pyodbc
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    
    if not all([server, database, username, password]):
        raise ValueError("Missing SQL environment variables in centralized .env")

    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE={database};'
        f'UID={username};PWD={password};LoginTimeout=30'
    )
    return pyodbc.connect(conn_str)

def send_teams_notification(message, logger=None):
    """
    Standardized Adaptive Card notification for Power Automate.
    Matches the schema required by the Medallion Email Extraction workflow.
    """
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    ui_url = os.getenv("PREFECT_UI_URL", "http://10.1.50.126:4200")

    if not webhook_url:
        if logger: logger.warning("⚠️ No TEAMS_WEBHOOK_URL found. Skipping.")
        return

    # Dynamic Run Link Generation
    try:
        current_run_id = flow_run.get_id()
        run_link = f"{ui_url}/runs/flow-run/{current_run_id}" if current_run_id else ui_url
    except Exception:
        run_link = ui_url

    # Adaptive Card Payload (Strict Schema for triggerBody() parsing)
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": message,
                            "wrap": True,
                            "weight": "Bolder" if any(x in message for x in ["Failed", "❌", "⚠️"]) else "Default",
                            "color": "Attention" if any(x in message for x in ["Failed", "❌", "⚠️"]) else "Default"
                        }
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "🔍 View Flow Run",
                            "url": run_link
                        }
                    ],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.2"
                }
            }
        ]
    }
    
    try:
        response = r.post(webhook_url, json=payload)
        response.raise_for_status()
        if logger: logger.info("✅ Teams Notification Sent.")
    except Exception as e:
        if logger: logger.error(f"❌ Teams Webhook Failed: {e}")
