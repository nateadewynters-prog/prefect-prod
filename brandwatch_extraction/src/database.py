import os
import pyodbc
import json
import time
from prefect import get_run_logger
from src.notifications import send_teams_notification

def get_db_connection(retries=3, delay=5):
    """Attempts to connect to Azure SQL with a retry mechanism."""
    logger = get_run_logger()
    db_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server=tcp:{os.getenv('SQL_SERVER')},1433;"
        f"Database={os.getenv('SQL_ORGANICSOCIAL_DATABASE')};"
        f"Uid={os.getenv('SQL_USERNAME')};Pwd={os.getenv('SQL_PASSWORD')};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    
    for attempt in range(retries):
        try:
            return pyodbc.connect(db_str)
        except pyodbc.OperationalError as e:
            if "HYT00" in str(e) and attempt < retries - 1:
                logger.warning(f"Database connection timeout. Retrying in {delay}s... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                error_msg = f"Database Connection Failed: {str(e)}"
                logger.error(f"❌ {error_msg}")
                send_teams_notification(f"🚨 **Database Connection Error**\n\n{error_msg}", logger)
                raise e

def insert_raw_json(endpoint_tag, raw_data):
    """Inserts API JSON payloads directly into the staging table."""
    logger = get_run_logger()
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 🚀 THE FIX: Restored your exact table name and columns!
            query = """
                INSERT INTO dbo.stg_bw_raw_json (SourceEndpoint, RawData) 
                VALUES (?, ?)
            """
            
            # Dump dict to string for the JSON/NVARCHAR column
            json_payload = json.dumps(raw_data)
            
            cursor.execute(query, (endpoint_tag, json_payload))
            conn.commit()
            
            logger.info(f"💾 Successfully staged {endpoint_tag} data to SQL.")
            
    except Exception as e:
        # Catch specific database errors (like the invalid object name we just saw)
        error_msg = f"SQL Insertion Failed for {endpoint_tag}: {str(e)}"
        logger.error(f"❌ {error_msg}")
        send_teams_notification(f"🚨 **Brandwatch Database Error**\n\n{error_msg}", logger)
        raise
