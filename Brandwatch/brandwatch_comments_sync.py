import os
import requests as r
import pandas as pd
import pyodbc
import time
from io import StringIO
from datetime import datetime, timedelta, date, timezone
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# --- Configuration ---
API_KEY = 'MSgkkt0njOWJf1qD8tUYzeCAmnySSQeG'
BASE_URL = "https://api.falcon.io"
OUTPUT_DIR = r'C:\BrandwatchOutputs\comment'

# Teams Webhook URL
TEAMS_WEBHOOK_URL = "https://dewyntersltd.webhook.office.com/webhookb2/df24aee1-6e35-4412-a954-62d7005cb565@93974508-dded-498b-9c98-7933dd4b0ffa/IncomingWebhook/f7c6f946322e4743b73909889d17bb69/4d94369e-f4c5-4157-8234-21533c1e276c/V2_hG_R7DI3Z9dTZNhgriHkcrG6uh2lO81bBhD3VbRuok1"

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# --- Helper Functions ---

def send_teams_notification(message, logger):
    """Sends a notification to MS Teams via Webhook."""
    if not TEAMS_WEBHOOK_URL:
        logger.warning("No Teams Webhook URL provided. Skipping notification.")
        return

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": message,
                            "wrap": True,
                            "weight": "Bolder" if "Failed" in message else "Default",
                            "color": "Attention" if "Failed" in message else "Default"
                        }
                    ],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.2"
                }
            }
        ]
    }
    
    try:
        response = r.post(TEAMS_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        logger.info("✅ Teams Notification Sent.")
    except Exception as e:
        logger.error(f"❌ Failed to send Teams notification: {e}")

# --- Tasks ---

@task(name="fetch_comments_data", retries=3, retry_delay_seconds=60)
def fetch_comments_data():
    logger = get_run_logger()
    
    # Logic: 2 days ago
    two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
    start_date = two_days_ago.strftime('%Y-%m-%dT00:00:00.000Z')
    end_date = two_days_ago.strftime('%Y-%m-%dT23:59:59.999Z')
    date_str = two_days_ago.strftime('%y%m%d')
    
    logger.info(f"Fetching comments for date: {date_str} ({start_date} to {end_date})")

    # 1. Request Export UUID
    uuid_request_url = f"{BASE_URL}/engage/v2/exports"
    params = {'apikey': API_KEY}
    json_data = {
        'startDate': start_date,
        'endDate': end_date,
        'networks': ['facebook', 'instagram', 'youtube', 'whatsapp', 'twitter', 'tiktok', 'linkedin', 'googlemybusiness'],
        'types': ['comment', 'reply', 'dm']
    }
    headers = {'Content-Type': 'application/json'}

    try:
        req = r.post(uuid_request_url, params=params, json=json_data, headers=headers)
        req.raise_for_status()
        comment_uuid = req.json().get('uuid')
        logger.info(f"Export Job Initiated. UUID: {comment_uuid}")
    except Exception as e:
        logger.error(f"Failed to initiate export: {e}")
        raise

    # 2. Poll for Completion
    poll_url = f"{BASE_URL}/engage/v2/exports/{comment_uuid}"
    max_retries = 30  # 30 * 10s = 5 minutes max wait
    
    for attempt in range(max_retries):
        try:
            res = r.get(poll_url, params=params, headers=headers).json()
            status = res.get('status', 'UNKNOWN')
            
            if status == 'COMPLETED':
                export_url = res.get('url')
                logger.info("Export COMPLETED. Downloading data...")
                
                # 3. Download and Save
                data_res = r.get(export_url)
                if data_res.status_code == 200:
                    csv_content = data_res.text
                    
                    # Validate empty data
                    if not csv_content.strip():
                        logger.warning("Export returned empty content.")
                        return None
                        
                    df = pd.read_csv(StringIO(csv_content))
                    
                    os.makedirs(OUTPUT_DIR, exist_ok=True)
                    file_path = os.path.join(OUTPUT_DIR, f'comments_export_{date_str}.csv')
                    df.to_csv(file_path, index=False)
                    
                    logger.info(f"Saved: {file_path} | Rows: {len(df)}")
                    return file_path
                else:
                    raise Exception(f"Download failed with status {data_res.status_code}")
            
            elif status == 'FAILED':
                raise Exception("Brandwatch Export Job FAILED.")
            
            else:
                # Still processing
                time.sleep(10)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(10)

    raise TimeoutError("Export polling timed out.")

@task(name="push_comments_to_sql")
def push_comments_to_sql(file_path):
    logger = get_run_logger()
    
    if not file_path or not os.path.exists(file_path):
        logger.warning("No file provided or file does not exist.")
        return 0

    # SQL Config
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};LoginTimeout=30'

    # Read and Clean Data
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"Reading CSV: {len(df)} rows.")
        
        if df.empty:
            logger.warning("DataFrame is empty. Skipping.")
            return 0

        # Clean columns: Replace spaces/parens with underscores
        df.columns = [col.replace(' ', '_').replace('(', '').replace(')', '') for col in df.columns]

        # Drop unwanted columns (from legacy script)
        drop_cols = ['Author_name', 'Falcon_user_name', 'Author_follower_count', 'Audience_labels']
        df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

        # Type Conversion
        if 'Date_created_UTC' in df.columns:
            df['Date_created_UTC'] = pd.to_datetime(df['Date_created_UTC'], errors='coerce')
        
        # Numeric conversions (handle NaN -> 0 -> int)
        for col in ['Number_of_likes', 'Comment_and_reply_count']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    except Exception as e:
        logger.error(f"Data preparation failed: {e}")
        raise

    # Prepare Data List
    data_to_insert = []
    for _, row in df.iterrows():
        # Clean tuple generation: Handle NaN -> None
        row_values = [None if pd.isnull(val) else val for val in row.values]
        data_to_insert.append(tuple(row_values))

    if not data_to_insert:
        return 0

    # SQL Execution
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    rows_inserted = 0

    try:
        # 1. Idempotency: Delete existing comments for this date range to prevent duplicates
        if 'Date_created_UTC' in df.columns:
            # Get min/max date from the dataset to scope the delete
            min_date = df['Date_created_UTC'].min()
            max_date = df['Date_created_UTC'].max()
            
            if pd.notnull(min_date) and pd.notnull(max_date):
                # Expand slightly to cover the full day boundaries if needed, 
                # or just delete based on the exact IDs if an ID column exists.
                # Assuming 'Date_created_UTC' is the safest partitioning key for this daily logic.
                
                # Convert to string for SQL
                min_str = min_date.strftime('%Y-%m-%d %H:%M:%S')
                max_str = max_date.strftime('%Y-%m-%d %H:%M:%S')
                
                logger.info(f"Deleting existing records between {min_str} and {max_str}...")
                cursor.execute("DELETE FROM bwComment WHERE Date_created_UTC BETWEEN ? AND ?", min_str, max_str)
        
        # 2. Bulk Insert
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?'] * len(df.columns))
        sql = f"INSERT INTO bwComment ({columns}) VALUES ({placeholders})"
        
        cursor.executemany(sql, data_to_insert)
        conn.commit()
        
        rows_inserted = len(data_to_insert)
        logger.info(f"Successfully inserted {rows_inserted} rows.")
        
        # 3. Verification
        logger.info("--- SQL Verification: Last 7 Days ---")
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        verify_sql = f"""
            SELECT CAST(Date_created_UTC AS DATE), COUNT(*) 
            FROM bwComment 
            WHERE Date_created_UTC >= '{seven_days_ago}' 
            GROUP BY CAST(Date_created_UTC AS DATE) 
            ORDER BY CAST(Date_created_UTC AS DATE) DESC
        """
        cursor.execute(verify_sql)
        for row in cursor.fetchall():
            logger.info(f"Date: {row[0]} | Count: {row[1]}")
            
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        raise
    finally:
        conn.close()
        
    return rows_inserted

@flow(name="Brandwatch Comments Sync", log_prints=True)
def brandwatch_comments_flow():
    logger = get_run_logger()
    
    try:
        # 1. Fetch
        csv_path = fetch_comments_data()
        
        # 2. Push
        total_rows = push_comments_to_sql(csv_path)
        
        # 3. Success Notification
        send_teams_notification(
            f"✅ **Brandwatch Comments Sync Successful**\n\n"
            f"**Date:** {date.today()}\n"
            f"**Rows Inserted:** {total_rows}",
            logger
        )
        
    except Exception as e:
        # 4. Failure Notification
        error_msg = str(e)
        send_teams_notification(
            f"❌ **Brandwatch Comments Sync Failed**\n\n"
            f"**Error:** {error_msg}",
            logger
        )
        raise e

if __name__ == "__main__":
    brandwatch_comments_flow.serve(
        name="brandwatch-comments-daily",
        cron="30 9 * * *",
        tags=["brandwatch", "comments", "production"]
    )