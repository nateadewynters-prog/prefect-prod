import os
import requests as r
import pandas as pd
import pyodbc
from time import sleep
from datetime import datetime, timedelta, date
from prefect import flow, task, get_run_logger

# Import Shared Utils
import brandwatch_utils as utils

# --- Configuration ---
utils.setup_environment()
API_KEY = '1SfCwWj7AAlGBPSgQFDC5Bf9PBLz6wsn' 
OUTPUT_DIR = r'C:\BrandwatchOutputs\channel'

@task(name="fetch_brandwatch_data", retries=3, retry_delay_seconds=300)
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
    os.makedirs(r'C:\BrandwatchOutputs\channellist', exist_ok=True)
    pd.DataFrame([{
        'channelId': ch['uuid'],
        'channel_name': ch['name'],
        'network': ch['network']
    } for ch in channels]).to_csv(r'C:\BrandwatchOutputs\channellist\channels_list.csv', index=False)

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
    if not os.path.exists(file_path): return 0

    try:
        df = pd.read_csv(file_path)
        if df.empty: return 0
    except Exception: return 0

    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '') for c in df.columns]
    data_to_insert = [tuple(x if pd.notnull(x) else None for x in r) for r in df.values]
    if not data_to_insert: return 0

    conn = None
    rows_inserted = 0
    
    for attempt in range(5):
        try:
            conn = utils.get_db_connection()
            cursor = conn.cursor()
            
            placeholders = ', '.join(['?'] * len(df.columns))
            sql = f"INSERT INTO bwChannel ({', '.join(df.columns)}) VALUES ({placeholders})"
            cursor.executemany(sql, data_to_insert)
            conn.commit()
            
            rows_inserted = len(data_to_insert)
            logger.info(f"✅ SQL Import Successful: {rows_inserted} rows.")
            break
        except pyodbc.Error as e:
            logger.warning(f"SQL Insert failed (Attempt {attempt+1}): {e}")
            if attempt < 4: sleep(5 * (2 ** attempt))
            else: raise
        finally:
            if conn: conn.close()
                
    return rows_inserted

@flow(name="Brandwatch Channel Sync", log_prints=True)
def brandwatch_sync_flow():
    logger = get_run_logger()
    try:
        csv_path = fetch_brandwatch_data()
        total_rows = push_to_sql(csv_path)
        utils.send_teams_notification(
            f"✅ **Brandwatch Channel Sync Successful**\n\n**Date:** {date.today()}\n**Rows:** {total_rows}", logger
        )
    except Exception as e:
        utils.send_teams_notification(f"❌ **Brandwatch Channel Sync Failed**\n\n**Error:** {str(e)}", logger)
        raise e

if __name__ == "__main__":
    brandwatch_sync_flow.serve(name="brandwatch-channel-daily", cron="0 7 * * *", tags=["brandwatch", "channel"])