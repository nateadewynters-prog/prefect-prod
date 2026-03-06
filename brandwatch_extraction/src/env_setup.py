import os
from dotenv import load_dotenv
from pathlib import Path

# Path to the centralized .env file on the VM
ENV_PATH = Path("/opt/prefect/prod/.env")

def setup_environment():
    """Loads the centralized .env file for Brandwatch."""
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)
        print(f"✅ Brandwatch environment loaded from {ENV_PATH}")
    else:
        print(f"⚠️ CRITICAL: .env not found at {ENV_PATH}")
