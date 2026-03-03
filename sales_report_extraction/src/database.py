import os
import pyodbc
from prefect import get_run_logger
from src.notifications import send_teams_notification # Added import

def get_db_connection():
    """Returns an active pyodbc connection using centralized env vars."""
    # Attempt to get the Prefect logger, fallback to None if outside a flow context
    try:
        logger = get_run_logger()
    except Exception:
        logger = None

    server = os.getenv('SQL_SERVER') #
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE') #
    username = os.getenv('SQL_USERNAME') #
    password = os.getenv('SQL_PASSWORD') #
    
    if not all([server, database, username, password]): #
        error_msg = "Missing SQL environment variables in centralized .env"
        if logger: logger.error(f"❌ {error_msg}")
        send_teams_notification(f"🚨 **Database Config Error**\n\n{error_msg}", logger) # Added alert
        raise ValueError(error_msg) #

    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE={database};'
        f'UID={username};PWD={password};LoginTimeout=30'
    ) #
    
    try:
        conn = pyodbc.connect(conn_str) #
        if logger: logger.info(f"✅ Successfully connected to SQL Database '{database}' on '{server}'") # Added visibility
        return conn #
    except Exception as e:
        error_msg = f"Failed to connect to SQL Database '{database}' on '{server}': {str(e)}"
        if logger: logger.error(f"❌ {error_msg}")
        send_teams_notification(f"🚨 **Database Connection Failed**\n\n{error_msg}", logger) # Added critical alert
        raise #