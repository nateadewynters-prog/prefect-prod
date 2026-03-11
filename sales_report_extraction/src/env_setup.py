import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from src.notifications import send_teams_notification 

ENV_PATH = Path("/opt/prefect/prod/.env") 

def setup_environment():
    """Loads the centralized .env file from the Prefect root."""
    if ENV_PATH.exists(): 
        load_dotenv(ENV_PATH) 
        print(f"✅ Environment variables loaded from {ENV_PATH}") 
    else:
        error_msg = f"⚠️ CRITICAL: .env not found at {ENV_PATH}"
        print(error_msg) 
        
        # 🚀 FIX: Create a standard logger so the notification script doesn't crash
        fallback_logger = logging.getLogger("EnvSetup")
        
        send_teams_notification(
            message="⚠️ **CRITICAL: Configuration Missing**",
            logger=fallback_logger,
            facts={"Expected Path": str(ENV_PATH), "Issue": ".env file not found!"}
        )