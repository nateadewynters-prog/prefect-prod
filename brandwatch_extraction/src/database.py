import os
import pyodbc
import json
from prefect import get_run_logger

def get_db_connection():
    # Uses environment variables mounted via docker-compose
    db_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server=tcp:{os.getenv('SQL_SERVER')},1433;"
        f"Database={os.getenv('SQL_ORGANICSOCIAL_DATABASE')};"
        f"Uid={os.getenv('SQL_USERNAME')};Pwd={os.getenv('SQL_PASSWORD')};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(db_str)

def insert_raw_json(endpoint_tag, raw_data):
    logger = get_run_logger()
    try:
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
