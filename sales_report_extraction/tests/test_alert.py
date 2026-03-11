import logging
from src.env_setup import setup_environment
from src.notifications import send_teams_notification

# Create a basic logger to mimic Prefect's get_run_logger()
logging.basicConfig(level=logging.DEBUG)
mock_logger = logging.getLogger("TestAlert")

def test_webhook():
    # 1. Load the .env file so it grabs your TEAMS_WEBHOOK_URL
    setup_environment()
    
    # 2. Create some fake data to populate the FactSet table
    error_facts = {
        "Rule": "TICKETEK_JESUS_CHRIST_SUPERSTAR_MANILA",
        "Show": "Jesus Christ Superstar",
        "Venue": "Manila",
        "Error Details": "ValueError: Unmapped codes found {'VIP-PKG', 'COMP'}"
    }
    
    print("🚀 Firing test alert to Teams...")
    
    # 3. Send the alert using your newly upgraded function
    send_teams_notification(
        message="⚠️ **Action Required: Data Mapping Failed**\n\nPlease update the local lookup CSV on the server and remove the 'sales_report_failed' tag in Outlook to replay.",
        logger=mock_logger,
        facts=error_facts
    )
    
    print("✅ Done! Check your Teams channel.")

if __name__ == "__main__":
    test_webhook()
