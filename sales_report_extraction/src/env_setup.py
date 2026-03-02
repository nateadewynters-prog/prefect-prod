import os
from dotenv import load_dotenv
from pathlib import Path
from src.notifications import send_teams_notification # Added import

# Resolves to the centralized config regardless of which script calls it
ENV_PATH = Path("/opt/prefect/prod/.env") #

def setup_environment():
    """Loads the centralized .env file from the Prefect root."""
    if ENV_PATH.exists(): #
        load_dotenv(ENV_PATH) #
        print(f"✅ Environment variables loaded from {ENV_PATH}") # Added visibility
    else:
        # We use standard print here because Prefect logger hasn't initialized yet
        error_msg = f"⚠️ CRITICAL: .env not found at {ENV_PATH}"
        print(error_msg) 
        send_teams_notification(error_msg) # Added critical alert