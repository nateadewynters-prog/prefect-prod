import os
import requests as r
from prefect.runtime import flow_run

# 🚀 THE FIX: Added the facts dictionary parameter
def send_teams_notification(message: str, logger=None, facts: dict = None):
    """
    Standardized Adaptive Card notification.
    Supports dynamic FactSets (tables) for easy reading.
    """
    webhook_url = os.getenv("TEAMS_WEBHOOK_DEV")
    ui_url = os.getenv("PREFECT_UI_URL", "http://10.1.50.127:4200")

    if not webhook_url:
        if logger: logger.warning("⚠️ No TEAMS_WEBHOOK_URL found. Skipping.")
        return

    try:
        current_run_id = flow_run.get_id()
        run_link = f"{ui_url}/runs/flow-run/{current_run_id}" if current_run_id else ui_url
    except Exception:
        run_link = ui_url

    # 1. Build the core message block
    body_elements = [
        {
            "type": "TextBlock",
            "text": message,
            "wrap": True,
            "weight": "Bolder" if any(x in message for x in ["Failed", "❌", "⚠️", "🚨"]) else "Default",
            "color": "Attention" if any(x in message for x in ["Failed", "❌", "⚠️", "🚨"]) else "Default"
        }
    ]

    # 2. 🚀 THE NEW FEATURE: Inject the FactSet if facts are provided
    if facts:
        fact_list = [{"title": str(key), "value": str(value)} for key, value in facts.items()]
        body_elements.append({
            "type": "FactSet",
            "facts": fact_list
        })

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": body_elements,
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
