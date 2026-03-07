import os
import pyodbc
import json
import time
from prefect import get_run_logger

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
                raise e

def insert_raw_json(endpoint_tag, raw_data):
    logger = get_run_logger()
    try:
        # get_db_connection now handles the retry logic internally
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO stg_bw_raw_json (SourceEndpoint, RawData) VALUES (?, ?)", 
                endpoint_tag, json.dumps(raw_data)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Critical SQL Error for {endpoint_tag}: {e}")
        raise
