import os
import requests as r
from dotenv import load_dotenv
from prefect.runtime import flow_run

def setup_environment():
    """Loads .env file from the current directory."""
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Ensure environment variables are loaded immediately upon import
setup_environment()

# Safely load the configurations from the .env file
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")
PREFECT_UI_URL = os.getenv("PREFECT_UI_URL", "http://10.1.50.126:4200")

def send_teams_notification(message, logger=None):
    """Sends an Adaptive Card notification to MS Teams via a Workflows Webhook."""
    if not TEAMS_WEBHOOK_URL:
        if logger: logger.warning("⚠️ No Teams Webhook URL found in .env. Skipping notification.")
        return

    # Dynamic Run Link Generation
    try:
        current_run_id = flow_run.get_id()
        run_link = f"{PREFECT_UI_URL}/runs/flow-run/{current_run_id}" if current_run_id else PREFECT_UI_URL
    except Exception:
        run_link = PREFECT_UI_URL

    # Power Automate Workflows accept this standard Adaptive Card payload
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
        response = r.post(TEAMS_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        if logger: logger.info("✅ Teams Notification Sent.")
    except Exception as e:
        # This will now properly log 400/404 errors if the URL is ever retired again
        if logger: logger.error(f"❌ Failed to send Teams notification: {e}")