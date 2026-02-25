import sys
import os
from pathlib import Path
import requests as r
import pandas as pd
import pyodbc
from time import sleep
from datetime import datetime, timedelta, date
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact


# 2. Updated Import
import utils
from utils import ValidationResult

# --- Configuration ---
utils.setup_environment()
API_KEY = os.getenv('BRANDWATCH_API_KEY')
OUTPUT_DIR = '/opt/data/brandwatch_outputs/channel'

@task(name="fetch_brandwatch_data")
def fetch_brandwatch_data():
    logger = get_run_logger()
    target_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    since = until = target_date
    logger.info(f"Starting fetch for date: {target_date}")

    # --- Part 1: Fetch Channels ---
    endpoint_url = utils.BASE_URL + "/channels"
    params = {'apikey': API_KEY, 'limit': 1000}
    
    try:
        channels_call = r.get(endpoint_url, params=params).json()
        channels = channels_call.get('items', [])
    except Exception as e:
        logger.error(f"Failed to fetch channels: {e}")
        raise 

    # Export Channel List (Optional utility output)
    os.makedirs('/opt/data/brandwatch_outputs/channellist', exist_ok=True)
    pd.DataFrame([{
        'channelId': ch['uuid'],
        'channel_name': ch['name'],
        'network': ch['network']
    } for ch in channels]).to_csv('/opt/data/brandwatch_outputs/channellist/channels_list.csv', index=False)

    # --- Part 2: Fetch Metrics ---
    metrics = [
        'channel/impressions/day', 'channel/fans/day', 'channel/shares/day', 
        'channel/reactions/day', 'channel/comments/day', 'channel/reach/day'
    ]
    
    channel_chunks = [channels[x:x+15] for x in range(0, len(channels), 15)]
    all_channels_info = []

    for i, channel_chunk in enumerate(channel_chunks, 1):
        channel_request_url = utils.BASE_URL + "/measure/v2/insights/channel"
        payload = {
            'since': since, 'until': until, 'metricIds': metrics,
            'channelIds': [x['uuid'] for x in channel_chunk]
        }
        
        try:
            req = r.post(channel_request_url, params={'apikey': API_KEY}, json=payload, headers={'Content-Type': 'application/json'}).json()
            sleep(5) 
            
            req_id = req.get('insightsRequestId')
            if not req_id: continue

            # Poll for results
            poll_url = utils.BASE_URL + f"/measure/v2/insights/{req_id}"
            
            def fetch_page(url, token=None):
                p_params = {'apikey': API_KEY, 'page': token} if token else {'apikey': API_KEY}
                resp = r.get(url, params=p_params).json()
                data = resp.get('data', {}).get('insights', {})
                for m_id in metrics:
                    if m_id in data:
                        for row in data[m_id]:
                            row['metricId'] = m_id
                            all_channels_info.append(row)
                return resp.get('data', {}).get('paging', {}).get('nextPage')

            next_page = fetch_page(poll_url)
            while next_page:
                next_page = fetch_page(poll_url, next_page)

        except Exception as e:
            logger.error(f"Error processing chunk {i}: {e}")
            continue

    # --- Part 3: Aggregate ---
    report_map = {} 
    for info in all_channels_info:
        c_id = info['channelId']
        if c_id not in report_map:
            report_map[c_id] = {'channelId': c_id, 'total_impressions': 0, 'total_fans': 0, 'total_shares': 0, 'total_reactions': 0, 'total_comments': 0, 'total_reach': 0}
        
        val = info['value']
        m_id = info['metricId']
        key_map = {
            'channel/impressions/day': 'total_impressions', 'channel/fans/day': 'total_fans',
            'channel/shares/day': 'total_shares', 'channel/reactions/day': 'total_reactions',
            'channel/comments/day': 'total_comments', 'channel/reach/day': 'total_reach'
        }
        if m_id in key_map: report_map[c_id][key_map[m_id]] += val

    final_report = list(report_map.values())
    channel_lookup = {c['uuid']: c for c in channels}
    for item in final_report:
        ch = channel_lookup.get(item['channelId'])
        if ch:
            item['channel_name'] = ch['name']
            item['network'] = ch['network']

    df = pd.DataFrame(final_report)
    df['date'] = target_date

    if df.empty:
        raise ValueError("Data frame is empty. Triggering retry.")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f'channel_metrics_{target_date}.csv')
    df.to_csv(output_file, index=False)
    return output_file

@task(name="push_to_sql")
def push_to_sql(file_path):
    logger = get_run_logger()
    if not os.path.exists(file_path): 
        return 0, ValidationResult("FAILED", "CSV File missing.", {})

    try:
        df = pd.read_csv(file_path)
        if df.empty: 
            return 0, ValidationResult("FAILED", "CSV File is empty.", {})
    except Exception as e: 
        return 0, ValidationResult("FAILED", f"Error reading CSV: {e}", {})

    # Sanitize Columns
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '') for c in df.columns]
    
    # Prepare data for insertion
    data_to_insert = [tuple(x if pd.notnull(x) else None for x in r) for r in df.values]
    if not data_to_insert: 
        return 0, ValidationResult("FAILED", "No valid data to insert.", {})

    # IDEMPOTENCY FIX: Extract the date to ensure we can clear old data
    target_date = df['date'].iloc[0]

    conn = None
    rows_inserted = 0
    
    for attempt in range(5):
        try:
            conn = utils.get_db_connection()
            cursor = conn.cursor()
            
            # --- START TRANSACTION ---
            logger.info(f"🧹 Clearing existing data for {target_date}...")
            cursor.execute("DELETE FROM bwChannel WHERE date = ?", target_date)
            
            placeholders = ', '.join(['?'] * len(df.columns))
            sql = f"INSERT INTO bwChannel ({', '.join(df.columns)}) VALUES ({placeholders})"
            cursor.executemany(sql, data_to_insert)
            
            conn.commit()
            rows_inserted = len(data_to_insert)
            logger.info(f"✅ SQL Import Successful: {rows_inserted} rows inserted for {target_date}.")
            
            val_result = ValidationResult(
                status="PASSED",
                message="✅ Channel metrics successfully updated.",
                metrics={"Target Date": target_date, "Rows Inserted": rows_inserted}
            )
            return rows_inserted, val_result
            
        except pyodbc.Error as e:
            logger.warning(f"SQL Insert failed (Attempt {attempt+1}): {e}")
            if conn: conn.rollback()
            if attempt < 4: sleep(5 * (2 ** attempt))
            else: 
                return 0, ValidationResult("FAILED", f"SQL Error: {str(e)}", {})
        finally:
            if conn: conn.close()
                
    return 0, ValidationResult("FAILED", "Exhausted retries.", {})

@flow(name="Brandwatch Channel Sync", log_prints=True)
def brandwatch_sync_flow():
    logger = get_run_logger()
    try:
        csv_path = fetch_brandwatch_data()
        total_rows, val_result = push_to_sql(csv_path)
        
        # Hard Fail Check
        if val_result.status == "FAILED":
            raise ValueError(f"Data Validation Failed: {val_result.message}")

        # Artifact Generation
        md_table = f"## Validation Result: {val_result.status}\n\n**Message:** {val_result.message}\n\n| Metric | Value |\n|---|---|\n"
        for key, val in val_result.metrics.items(): md_table += f"| {key} | {val} |\n"
        create_markdown_artifact(key="brandwatch-channel-val", markdown=md_table, description="Channel Sync Validation")

        # Alert Logic
        if val_result.status == "UNVALIDATED":
            utils.send_teams_notification(f"⚠️ **Brandwatch Channel: Manual Review**\n\n{val_result.message}", logger)
        else:
            logger.info("✅ Teams Alert Bypassed: Validation PASSED natively.")

    except Exception as e:
        utils.send_teams_notification(f"❌ **Brandwatch Channel Sync Failed**\n\n**Error:** {str(e)}", logger)
        raise e

if __name__ == "__main__":
    brandwatch_sync_flow.serve(name="brandwatch-channel-daily", cron="0 7 * * *", tags=["brandwatch", "channel"])