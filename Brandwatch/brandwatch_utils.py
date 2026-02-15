import os
import pyodbc
import requests as r
from dotenv import load_dotenv
from prefect.runtime import flow_run  # [!code ++]

BASE_URL = "https://api.falcon.io"
TEAMS_WEBHOOK_URL = "https://dewyntersltd.webhook.office.com/webhookb2/df24aee1-6e35-4412-a954-62d7005cb565@93974508-dded-498b-9c98-7933dd4b0ffa/IncomingWebhook/f7c6f946322e4743b73909889d17bb69/4d94369e-f4c5-4157-8234-21533c1e276c/V2_hG_R7DI3Z9dTZNhgriHkcrG6uh2lO81bBhD3VbRuok1"
PREFECT_UI_URL = "http://10.1.50.126:4200" # [!code ++]

def setup_environment():
    """Loads .env file from the current directory."""
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

def get_sql_conn_str():
    """Returns the SQL connection string using env vars."""
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    
    if not all([server, database, username, password]):
        raise ValueError("Missing SQL environment variables in .env file")

    return (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};DATABASE={database};'
        f'UID={username};PWD={password};LoginTimeout=30'
    )

def get_db_connection():
    """Returns an active pyodbc connection."""
    conn_str = get_sql_conn_str()
    return pyodbc.connect(conn_str)

def send_teams_notification(message, logger=None):
    """Sends a notification to MS Teams via Webhook."""
    if not TEAMS_WEBHOOK_URL:
        if logger: logger.warning("No Teams Webhook URL provided. Skipping notification.")
        return

    # [!code ++] --- Dynamic Run Link Generation ---
    try:
        current_run_id = flow_run.get_id()
        run_link = f"{PREFECT_UI_URL}/runs/flow-run/{current_run_id}" if current_run_id else PREFECT_UI_URL
    except Exception:
        run_link = PREFECT_UI_URL
    # [!code ++] -----------------------------------

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
                            "weight": "Bolder" if "Failed" in message else "Default",
                            "color": "Attention" if "Failed" in message else "Default"
                        }
                    ],
                    # [!code ++] Add Action Button for Link
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
        r.post(TEAMS_WEBHOOK_URL, json=payload).raise_for_status()
        if logger: logger.info("✅ Teams Notification Sent.")
    except Exception as e:
        if logger: logger.error(f"❌ Failed to send Teams notification: {e}")