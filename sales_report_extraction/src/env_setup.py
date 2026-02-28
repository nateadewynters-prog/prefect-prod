import os
from dotenv import load_dotenv
from pathlib import Path

# Resolves to the centralized config regardless of which script calls it
ENV_PATH = Path("/opt/prefect/prod/.env")

def setup_environment():
    """Loads the centralized .env file from the Prefect root."""
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)
    else:
        print(f"⚠️ Warning: .env not found at {ENV_PATH}")
