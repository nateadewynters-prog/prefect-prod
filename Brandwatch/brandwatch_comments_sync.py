import os
import requests as r
import pandas as pd
import pyodbc
import time
from io import StringIO
from datetime import datetime, timedelta, date, timezone
from prefect import flow, task, get_run_logger

# Import Shared Utils
import brandwatch_utils as utils

# --- Configuration ---
utils.setup_environment()
API_KEY = 'MSgkkt0njOWJf1qD8tUYzeCAmnySSQeG'
OUTPUT_DIR = r'C:\BrandwatchOutputs\comment'

@task(name="fetch_comments_data", retries=3, retry_delay_seconds=60)
def fetch_comments_data():
    logger = get_run_logger()
    
    two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
    start_date = two_days_ago.strftime('%Y-%m-%dT00:00:00.000Z')
    end_date = two_days_ago.strftime('%Y-%m-%dT23:59:59.999Z')
    
    # 1. Initiate Export
    try:
        req = r.post(f"{utils.BASE_URL}/engage/v2/exports", params={'apikey': API_KEY}, json={
            'startDate': start_date, 'endDate': end_date,
            'networks': ['facebook', 'instagram', 'youtube', 'whatsapp', 'twitter', 'tiktok', 'linkedin', 'googlemybusiness'],
            'types': ['comment', 'reply', 'dm']
        }, headers={'Content-Type': 'application/json'})
        req.raise_for_status()
        comment_uuid = req.json().get('uuid')
        logger.info(f"Export Job Initiated. UUID: {comment_uuid}")
    except Exception as e:
        logger.error(f"Failed to initiate export: {e}")
        raise

    # 2. Poll
    poll_url = f"{utils.BASE_URL}/engage/v2/exports/{comment_uuid}"
    for _ in range(30):
        try:
            res = r.get(poll_url, params={'apikey': API_KEY}).json()
            status = res.get('status', 'UNKNOWN')
            
            if status == 'COMPLETED':
                data_res = r.get(res.get('url'))
                if data_res.status_code == 200:
                    df = pd.read_csv(StringIO(data_res.text))
                    os.makedirs(OUTPUT_DIR, exist_ok=True)
                    file_path = os.path.join(OUTPUT_DIR, f'comments_export_{two_days_ago.strftime("%y%m%d")}.csv')
                    df.to_csv(file_path, index=False)
                    return file_path
                raise Exception(f"Download failed: {data_res.status_code}")
            elif status == 'FAILED':
                raise Exception("Brandwatch Export Job FAILED.")
            time.sleep(10)
        except Exception as e:
            if "Export Job FAILED" in str(e): raise e
            time.sleep(10)
    raise TimeoutError("Export polling timed out.")

@task(name="push_comments_to_sql")
def push_comments_to_sql(file_path):
    logger = get_run_logger()
    if not file_path or not os.path.exists(file_path): return 0

    try:
        df = pd.read_csv(file_path)
        if df.empty: return 0

        df.columns = [col.replace(' ', '_').replace('(', '').replace(')', '') for col in df.columns]
        df = df.drop(columns=['Author_name', 'Falcon_user_name', 'Author_follower_count', 'Audience_labels'], errors='ignore')
        if 'Date_created_UTC' in df.columns:
            df['Date_created_UTC'] = pd.to_datetime(df['Date_created_UTC'], errors='coerce')
        for col in ['Number_of_likes', 'Comment_and_reply_count']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    except Exception as e:
        logger.error(f"Data preparation failed: {e}")
        raise

    data_to_insert = [tuple(None if pd.isnull(val) else val for val in row) for row in df.values]
    if not data_to_insert: return 0

    conn = utils.get_db_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True

    try:
        if 'Date_created_UTC' in df.columns and pd.notnull(df['Date_created_UTC'].min()):
            min_s = df['Date_created_UTC'].min().strftime('%Y-%m-%d %H:%M:%S')
            max_s = df['Date_created_UTC'].max().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("DELETE FROM bwComment WHERE Date_created_UTC BETWEEN ? AND ?", min_s, max_s)
        
        placeholders = ', '.join(['?'] * len(df.columns))
        sql = f"INSERT INTO bwComment ({', '.join(df.columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, data_to_insert)
        conn.commit()
        return len(data_to_insert)
    finally:
        conn.close()

@flow(name="Brandwatch Comments Sync", log_prints=True)
def brandwatch_comments_flow():
    logger = get_run_logger()
    try:
        csv_path = fetch_comments_data()
        total_rows = push_comments_to_sql(csv_path)
        utils.send_teams_notification(
            f"✅ **Brandwatch Comments Sync Successful**\n\n**Date:** {date.today()}\n**Rows:** {total_rows}", logger
        )
    except Exception as e:
        utils.send_teams_notification(f"❌ **Brandwatch Comments Sync Failed**\n\n**Error:** {str(e)}", logger)
        raise e

if __name__ == "__main__":
    brandwatch_comments_flow.serve(name="brandwatch-comments-daily", cron="30 9 * * *", tags=["brandwatch", "comments"])