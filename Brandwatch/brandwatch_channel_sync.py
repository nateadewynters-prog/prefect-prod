import os
import requests as r
import pandas as pd
import pyodbc
from time import sleep
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# --- Configuration ---
API_KEY = '1SfCwWj7AAlGBPSgQFDC5Bf9PBLz6wsn' 
BASE_URL = "https://api.falcon.io"
OUTPUT_DIR = r'C:\BrandwatchOutputs\channel'

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

@task(name="fetch_brandwatch_data", retries=3, retry_delay_seconds=300)
def fetch_brandwatch_data():
    logger = get_run_logger()
    
    # Calculate "Yesterday" (Logic preserved: 2 days ago)
    target_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    since = until = target_date
    
    logger.info(f"Starting fetch for date: {target_date}")

    # --- Part 1: Fetch Channels ---
    endpoint_url = BASE_URL + "/channels"
    params = {'apikey': API_KEY, 'limit': 1000}
    
    try:
        channels_call = r.get(endpoint_url, params=params).json()
        channels = channels_call.get('items', [])
    except Exception as e:
        logger.error(f"Failed to fetch channels: {e}")
        raise # Triggers retry

    # Export Channel List
    os.makedirs(r'C:\BrandwatchOutputs\channellist', exist_ok=True)
    channels_list_df = pd.DataFrame([{
        'channelId': ch['uuid'],
        'channel_name': ch['name'],
        'network': ch['network']
    } for ch in channels])
    
    channels_list_df.to_csv(r'C:\BrandwatchOutputs\channellist\channels_list.csv', index=False)
    del channels_list_df

    # --- Part 2: Fetch Metrics ---
    metrics = [
        'channel/impressions/day', 'channel/fans/day', 'channel/shares/day', 
        'channel/reactions/day', 'channel/comments/day', 'channel/reach/day'
    ]
    
    channel_chunks = [channels[x:x+15] for x in range(0, len(channels), 15)]
    all_channels_info = []
    
    logger.info(f"Processing {len(channel_chunks)} chunks of channels...")

    for i, channel_chunk in enumerate(channel_chunks, 1):
        channel_request_url = BASE_URL + "/measure/v2/insights/channel"
        params = {'apikey': API_KEY}
        payload = {
            'since': since,
            'until': until,
            'metricIds': metrics,
            'channelIds': [x['uuid'] for x in channel_chunk]
        }
        headers = {'Content-Type': 'application/json'}
        
        try:
            channel_request = r.post(channel_request_url, params=params, json=payload, headers=headers).json()
            sleep(5) 
            
            insights_request_id = channel_request.get('insightsRequestId')
            if not insights_request_id:
                logger.warning(f"No Request ID for chunk {i}")
                continue

            # Poll for results
            channel_fetch_metrics_url = BASE_URL + f"/measure/v2/insights/{insights_request_id}"
            
            def fetch_page(url, page_token=None):
                p_params = {'apikey': API_KEY}
                if page_token:
                    p_params['page'] = page_token
                
                resp = r.get(url, params=p_params).json()
                data_insights = resp.get('data', {}).get('insights', {})
                
                for metric_id in metrics:
                    if metric_id in data_insights:
                        for row in data_insights[metric_id]:
                            row['metricId'] = metric_id
                            all_channels_info.append(row)
                
                return resp.get('data', {}).get('paging', {}).get('nextPage')

            next_page = fetch_page(channel_fetch_metrics_url)
            while next_page:
                next_page = fetch_page(channel_fetch_metrics_url, next_page)

        except Exception as e:
            logger.error(f"Error processing chunk {i}: {e}")
            continue

    # --- Part 3: Aggregate Report ---
    logger.info("Aggregating report data...")
    
    report_map = {} 

    for info in all_channels_info:
        c_id = info['channelId']
        if c_id not in report_map:
            report_map[c_id] = {
                'channelId': c_id,
                'total_impressions': 0, 'total_fans': 0, 'total_shares': 0,
                'total_reactions': 0, 'total_comments': 0, 'total_reach': 0
            }
        
        val = info['value']
        m_id = info['metricId']
        
        if m_id == 'channel/impressions/day': report_map[c_id]['total_impressions'] += val
        elif m_id == 'channel/fans/day': report_map[c_id]['total_fans'] += val
        elif m_id == 'channel/shares/day': report_map[c_id]['total_shares'] += val
        elif m_id == 'channel/reactions/day': report_map[c_id]['total_reactions'] += val
        elif m_id == 'channel/comments/day': report_map[c_id]['total_comments'] += val
        elif m_id == 'channel/reach/day': report_map[c_id]['total_reach'] += val

    final_report = list(report_map.values())

    channel_lookup = {c['uuid']: c for c in channels}
    for item in final_report:
        ch = channel_lookup.get(item['channelId'])
        if ch:
            item['channel_name'] = ch['name']
            item['network'] = ch['network']

    final_report_df = pd.DataFrame(final_report)
    final_report_df['date'] = target_date

    # --- Retry Trigger: Check for Empty Data ---
    if final_report_df.empty:
        logger.warning("API returned 0 rows. This is unexpected.")
        raise ValueError("Data frame is empty. Triggering retry (Wait 5 mins)...")
    # -------------------------------------------
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f'channel_metrics_{target_date}.csv')
    final_report_df.to_csv(output_file, index=False)
    
    logger.info(f"CSV saved to {output_file} with {len(final_report_df)} rows")
    
    del final_report_df
    del all_channels_info
    return output_file

@task(name="push_to_sql")
def push_to_sql(file_path):
    logger = get_run_logger()
    
    # 1. Check if file exists
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return 0

    # Database Config
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};'
        f'UID={username};PWD={password};LoginTimeout=30'
    )

    # 2. Read CSV
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except pd.errors.EmptyDataError:
        logger.warning(f"The file {file_path} is completely empty. Skipping SQL push.")
        return 0

    # 3. Check for empty DataFrame
    if df.empty:
        logger.warning(f"No data found in {file_path}. Skipping SQL push.")
        return 0

    # Clean columns
    df.columns = [col.replace(' ', '_').replace('(', '').replace(')', '') for col in df.columns]

    # Prepare Data
    data_to_insert = []
    for _, row in df.iterrows():
        row_values = [None if pd.isnull(val) else val for val in row.values]
        data_to_insert.append(tuple(row_values))

    # 4. Final Safety Check before Insert
    if not data_to_insert:
        logger.warning("Data list is empty after processing. Skipping SQL push.")
        return 0

    logger.info(f"Inserting {len(data_to_insert)} rows into SQL...")

    conn = None
    max_retries = 5
    rows_inserted = 0
    
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str, timeout=30)
            cursor = conn.cursor()
            
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?'] * len(df.columns))
            sql_insert = f"INSERT INTO bwChannel ({columns}) VALUES ({placeholders})"
            
            cursor.executemany(sql_insert, data_to_insert)
            conn.commit()
            
            rows_inserted = len(data_to_insert)
            logger.info("SQL Import Successful.")
            
            # --- VERIFICATION LOG (Recent 7 Days) ---
            logger.info("--- SQL Verification: Last 7 Days Row Counts ---")
            
            seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            sql_verify = f"""
                SELECT date, COUNT(*) as row_count 
                FROM bwChannel 
                WHERE date >= '{seven_days_ago}' 
                GROUP BY date 
                ORDER BY date DESC
            """
            cursor.execute(sql_verify)
            rows = cursor.fetchall()
            
            if rows:
                for row in rows:
                    d_val = row[0]
                    c_val = row[1]
                    logger.info(f"Date: {d_val} | Count: {c_val}")
            else:
                logger.warning("No data found in the last 7 days.")
            
            logger.info("---------------------------------------------")
            # -----------------------------------------

            break
        except pyodbc.Error as e:
            logger.warning(f"SQL Connection/Insert failed (Attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                sleep(5 * (2 ** attempt))
            else:
                logger.error("Max retries reached. SQL push failed.")
                raise
        finally:
            if conn:
                conn.close()
                
    return rows_inserted

@flow(name="Brandwatch Channel Sync", log_prints=True)
def brandwatch_sync_flow():
    logger = get_run_logger()
    
    try:
        csv_path = fetch_brandwatch_data()
        total_rows = push_to_sql(csv_path)
        
        # --- SUCCESS NOTIFICATION ---
        send_teams_notification(
            f"✅ **Brandwatch Channel Sync Successful**\n\n"
            f"**Date:** {date.today()}\n"
            f"**Total Rows Inserted:** {total_rows}",
            logger
        )

    except Exception as e:
        # --- FAILURE NOTIFICATION ---
        error_message = str(e)
        send_teams_notification(
            f"❌ **Brandwatch Channel Sync Failed**\n\n"
            f"**Error:** {error_message}",
            logger
        )
        # Re-raise the exception so Prefect marks the flow run as Failed
        raise e

if __name__ == "__main__":
    brandwatch_sync_flow.serve(
        name="brandwatch-channel-daily",
        cron="0 7 * * *",  # 7:00 AM Daily
        tags=["brandwatch", "channel", "production"]
    )