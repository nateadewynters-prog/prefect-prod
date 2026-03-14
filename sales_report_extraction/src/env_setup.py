import os
import logging
from dotenv import load_dotenv
from pathlib import Path

ENV_PATH = Path("/opt/prefect/prod/.env")

def setup_environment():
    """Loads the centralized .env file from the Prefect root."""
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)
        print(f"✅ Environment variables loaded from {ENV_PATH}")
    else:
        print(f"⚠️ CRITICAL: .env not found at {ENV_PATH}")

def get_universal_logger(name=__name__):
    """
    KISS Best Practice: 
    Tries to grab the Prefect logger. If it fails (because you are testing locally),
    it builds a standard Python terminal logger so your script never crashes.
    """
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except Exception:
        logger = logging.getLogger(name)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger