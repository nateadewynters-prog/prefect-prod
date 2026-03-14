import os
import pyodbc
from src.env_setup import get_universal_logger

def get_db_connection():
    """Returns an active pyodbc connection using centralized env vars."""
    logger = get_universal_logger(__name__)

    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    
    if not all([server, database, username, password]): 
        error_msg = "Missing SQL environment variables in centralized .env"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE={database};'
        f'UID={username};PWD={password};LoginTimeout=30'
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        logger.info(f"✅ Successfully connected to SQL Database '{database}' on '{server}'")
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to connect to SQL Database '{database}' on '{server}': {str(e)}")
        raise