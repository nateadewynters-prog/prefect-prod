import os
import requests
from prefect.runtime import flow_run

def send_teams_notification(message: str, logger, facts: dict = None, button_title: str = None, button_url: str = None, channel: str = "dev"):
    """
    Sends a beautifully formatted Adaptive Card to Microsoft Teams.
    Routes to 'ops' or 'dev' channels based on the target audience.
    """
    # 1. 🚀 NEW: Dual-Channel Routing
    if channel == "ops":
        webhook_url = os.getenv("TEAMS_WEBHOOK_OPS")
    else:
        webhook_url = os.getenv("TEAMS_WEBHOOK_DEV")
        
    if not webhook_url:
        logger.warning(f"⚠️ Webhook URL for channel '{channel}' not set in .env. Skipping notification.")
        return

    # 2. Determine the color/theme based on the message content
    color = "Default" 
    if any(x in message for x in ["Failed", "❌", "⚠️", "Error", "Action Required"]):
        color = "Attention" # Red
    elif any(x in message for x in ["Complete", "Successful", "🏁", "✅"]):
        color = "Good" # Green

    # 3. Build the Core Text Block
    body_elements = [
        {
            "type": "TextBlock",
            "text": message,
            "wrap": True,
            "color": color,
            "weight": "Bolder" if color != "Default" else "Default"
        }
    ]

    # 4. Inject the FactSet if facts are provided
    if facts:
        fact_list = [{"title": str(key), "value": str(value)} for key, value in facts.items()]
        body_elements.append({
            "type": "FactSet",
            "facts": fact_list
        })

    # 5. Dynamic Action Buttons
    ui_url = os.getenv("PREFECT_UI_URL", "http://10.1.50.127:4200")
    
    try:
        current_run_id = flow_run.get_id()
        run_link = f"{ui_url}/runs/flow-run/{current_run_id}" if current_run_id else f"{ui_url}/dashboard"
    except Exception:
        run_link = f"{ui_url}/dashboard"

    actions = [
        {
            "type": "Action.OpenUrl",
            "title": "🔍 View Prefect Logs",
            "url": run_link
        }
    ]
    
    # If a specific fix button is provided, add it!
    if button_title and button_url:
        actions.append({
            "type": "Action.OpenUrl",
            "title": button_title,
            "url": button_url
        })

    # 6. Assemble the final Adaptive Card JSON payload
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

    # 7. Send it to Teams
    try:
        resp = requests.post(webhook_url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        logger.debug(f"📣 Teams notification sent successfully to '{channel}' channel.")
    except Exception as e:
        logger.error(f"❌ Failed to send Teams notification to '{channel}': {e}")
