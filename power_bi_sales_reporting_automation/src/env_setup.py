import os
from dotenv import load_dotenv
from pathlib import Path

def setup_environment():
    """Loads the centralized .env file from the Prefect root."""
    env_path = Path("/opt/prefect/prod/.env")
    if env_path.exists():
        load_dotenv(env_path)
    else:
        print(f"⚠️ WARNING: .env not found at {env_path}")