import os
import requests
from prefect import get_run_logger

def send_teams_notification(message: str, logger, facts: dict = None, button_title: str = None, button_url: str = None):
    """
    Sends a beautifully formatted Adaptive Card to Microsoft Teams.
    Supports dynamic FactSets (tables) and custom action buttons.
    """
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("⚠️ TEAMS_WEBHOOK_URL not set. Skipping notification.")
        return

    # 1. Determine the color/theme based on the message content
    color = "Default" 
    if any(x in message for x in ["Failed", "❌", "⚠️", "Error", "Action Required"]):
        color = "Attention" # Red
    elif any(x in message for x in ["Complete", "Successful", "🏁", "✅"]):
        color = "Good" # Green

    # 2. Build the Core Text Block
    body_elements = [
        {
            "type": "TextBlock",
            "text": message,
            "wrap": True,
            "color": color,
            "weight": "Bolder" if color != "Default" else "Default"
        }
    ]

    # 3. 🚀 THE NEW FEATURE: Inject the FactSet if facts are provided
    if facts:
        fact_list = [{"title": str(key), "value": str(value)} for key, value in facts.items()]
        body_elements.append({
            "type": "FactSet",
            "facts": fact_list
        })

    # 4. 🚀 THE NEW FEATURE: Dynamic Action Buttons
    actions = [
        {
            "type": "Action.OpenUrl",
            "title": "🔍 View Prefect Logs",
            "url": "https://app.prefect.cloud/" # Update this to your local Prefect UI if needed
        }
    ]
    
    # If a specific fix button is provided, add it!
    if button_title and button_url:
        actions.append({
            "type": "Action.OpenUrl",
            "title": button_title,
            "url": button_url
        })

    # 5. Assemble the final Adaptive Card JSON payload
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": body_elements,
                    "actions": actions
                }
            }
        ]
    }

    # 6. Send it to Teams
    try:
        resp = requests.post(webhook_url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        logger.debug("📢 Teams notification sent successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to send Teams notification: {e}")