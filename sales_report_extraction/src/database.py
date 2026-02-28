import os
import pyodbc

def get_db_connection():
    """Returns an active pyodbc connection using centralized env vars."""
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    
    if not all([server, database, username, password]):
        raise ValueError("Missing SQL environment variables in centralized .env")

    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE={database};'
        f'UID={username};PWD={password};LoginTimeout=30'
    )
    return pyodbc.connect(conn_str)
