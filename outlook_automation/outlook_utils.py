import os
import requests as r
from dotenv import load_dotenv
from prefect.runtime import flow_run

# Configuration Constants
# (Ensure these match your actual environment or .env values)
TEAMS_WEBHOOK_URL = "https://dewyntersltd.webhook.office.com/webhookb2/df24aee1-6e35-4412-a954-62d7005cb565@93974508-dded-498b-9c98-7933dd4b0ffa/IncomingWebhook/f7c6f946322e4743b73909889d17bb69/4d94369e-f4c5-4157-8234-21533c1e276c/V2_hG_R7DI3Z9dTZNhgriHkcrG6uh2lO81bBhD3VbRuok1"
PREFECT_UI_URL = "http://10.1.50.126:4200"

def setup_environment():
    """Loads .env file from the current directory."""
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

def send_teams_notification(message, logger=None):
    """Sends a notification to MS Teams via Webhook."""
    if not TEAMS_WEBHOOK_URL:
        if logger: logger.warning("No Teams Webhook URL provided. Skipping notification.")
        return

    # Dynamic Run Link Generation
    try:
        current_run_id = flow_run.get_id()
        run_link = f"{PREFECT_UI_URL}/runs/flow-run/{current_run_id}" if current_run_id else PREFECT_UI_URL
    except Exception:
        run_link = PREFECT_UI_URL

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
        r.post(TEAMS_WEBHOOK_URL, json=payload).raise_for_status()
        if logger: logger.info("✅ Teams Notification Sent.")
    except Exception as e:
        if logger: logger.error(f"❌ Failed to send Teams notification: {e}")