import os
import requests as r
from prefect.runtime import flow_run

def send_teams_notification(message: str, logger=None):
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

    # Adaptive Card Payload
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
