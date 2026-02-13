import os
import requests as r
import pandas as pd
import pyodbc
import time
import numpy as np
from datetime import datetime, timedelta, date
from collections import defaultdict
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# --- Configuration ---
API_KEY = '1SfCwWj7AAlGBPSgQFDC5Bf9PBLz6wsn' 
BASE_URL = "https://api.falcon.io"
OUTPUT_DIR = r'C:\BrandwatchOutputs\content'
STATUSES = "published"
TEAMS_WEBHOOK_URL = "https://dewyntersltd.webhook.office.com/webhookb2/df24aee1-6e35-4412-a954-62d7005cb565@93974508-dded-498b-9c98-7933dd4b0ffa/IncomingWebhook/f7c6f946322e4743b73909889d17bb69/4d94369e-f4c5-4157-8234-21533c1e276c/V2_hG_R7DI3Z9dTZNhgriHkcrG6uh2lO81bBhD3VbRuok1"

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# --- Helper Functions (Notifications & Logic) ---
def send_teams_notification(message, logger):
    """Sends a notification to MS Teams via Webhook."""
    if not TEAMS_WEBHOOK_URL:
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
        r.post(TEAMS_WEBHOOK_URL, json=payload).raise_for_status()
        logger.info("✅ Teams Notification Sent.")
    except Exception as e:
        logger.error(f"❌ Failed to send Teams notification: {e}")

def fetch_posts_logic(channel_map, target_date, logger):
    since = target_date.strftime('%Y-%m-%dT00:00:00.00Z')
    until = target_date.strftime('%Y-%m-%dT23:59:59.00Z')
    url = f"{BASE_URL}/publish/items"
    params = {"apikey": API_KEY, "statuses": STATUSES, "since": since, "until": until, "limit": 100}
    all_posts = []
    try:
        response = r.get(url, params=params)
        if response.status_code != 200: return []
        data = response.json()
        raw_posts = data.get('items', [])
        next_page = data.get('paging', {}).get('nextPage')
        while next_page:
            params["page"] = next_page
            resp = r.get(url, params=params)
            new_items = resp.json().get('items', [])
            raw_posts.extend(new_items)
            next_page = resp.json().get('paging', {}).get('nextPage')
        for post in raw_posts:
            if not str(post.get("facebook", {}) or post.get("instagram", {}) or post.get("tiktok", {})).startswith("{'dark': True"):
                all_posts.append(post)
        return all_posts
    except Exception as e:
        logger.error(f"Error fetching posts for {target_date}: {e}")
        return []

def get_insights_logic(channel_to_content, since_req, until_req, logger):
    req_url = f"{BASE_URL}/measure/v2/insights/content"
    headers = {"Content-Type": "application/json"}
    insight_request_ids = []
    channel_ids = list(channel_to_content.keys())
    channel_batches = [channel_ids[x:x+15] for x in range(0, len(channel_ids), 15)]
    for batch in channel_batches:
        all_content_pairs = []
        for ch_id in batch:
            for c_id in channel_to_content[ch_id]:
                all_content_pairs.append((ch_id, c_id))
        chunk_size = 300
        for i in range(0, len(all_content_pairs), chunk_size):
            chunk = all_content_pairs[i:i + chunk_size]
            payload_channels = []
            temp_map = defaultdict(list)
            for ch_id, c_id in chunk:
                temp_map[ch_id].append(c_id)
            for ch_id, c_ids in temp_map.items():
                payload_channels.append({'id': ch_id, 'contentIds': c_ids})
            body = {
                'since': since_req, 'until': until_req,
                'metricIds': [
                    'content/likes/lifetime', 'content/video_views/lifetime', 'content/comments/lifetime',
                    'content/shares/lifetime', 'content/clicks/lifetime', 'content/engagements/lifetime',
                    'content/impressions/lifetime', 'content/reactions/lifetime', 'content/user_follows/lifetime',
                    'content/views/lifetime', 'content/saves/lifetime', 'content/user_profile_clicks/lifetime',
                    'content/link_clicks/lifetime', 'content/reach/lifetime', 'content/frequency/lifetime'
                ],
                'channels': payload_channels
            }
            for attempt in range(3):
                try:
                    res = r.post(req_url, json=body, params={'apikey': API_KEY}, headers=headers).json()
                    req_id = res.get('insightsRequestId')
                    if req_id:
                        insight_request_ids.append(req_id)
                        break
                except Exception:
                    time.sleep(5)
    all_results = []
    pending = list(insight_request_ids)
    while pending:
        for req_id in pending[:]:
            poll_url = f"{BASE_URL}/measure/v2/insights/{req_id}"
            try:
                res = r.get(poll_url, params={'apikey': API_KEY}).json()
                status = res.get('status')
                if status == 'READY':
                    data = res.get('data', {}).get('insights', {})
                    for metric, items in data.items():
                        for item in items:
                            all_results.append({'contentId': item['contentId'], 'metric': metric, 'value': item['value']})
                    pending.remove(req_id)
                elif status == 'FAILED':
                    pending.remove(req_id)
            except:
                pass
            time.sleep(0.5)
        if pending: time.sleep(2)
    return all_results

# --- Tasks ---

@task(name="fetch_channels", retries=3)
def fetch_channels():
    logger = get_run_logger()
    url = f"{BASE_URL}/channels"
    try:
        res = r.get(url, params={'apikey': API_KEY, 'limit': 1000})
        items = res.json().get('items', [])
        logger.info(f"Fetched {len(items)} channels.")
        return {ch['uuid']: ch.get('name', 'No name') for ch in items}
    except Exception as e:
        logger.error(f"Failed to fetch channels: {e}")
        raise

@task(name="process_content_batch", retries=2)
def process_content_batch(batch_num, start_date, end_date, channel_map):
    logger = get_run_logger()
    logger.info(f"--- Starting Batch {batch_num}: {start_date.date()} to {end_date.date()} ---")
    all_posts = []
    all_results = []
    current_date = start_date
    while current_date <= end_date:
        posts = fetch_posts_logic(channel_map, current_date, logger)
        all_posts.extend(posts)
        channel_to_content = defaultdict(list)
        for post in posts:
            c_ids = post.get("channels", [])
            p_id = post.get("id")
            if c_ids and p_id:
                channel_to_content[c_ids[0]].append(p_id)
        if channel_to_content:
            since_req = current_date.strftime('%Y-%m-%dT00:00:00.00Z')
            until_req = current_date.strftime('%Y-%m-%dT23:59:59.00Z')
            results = get_insights_logic(channel_to_content, since_req, until_req, logger)
            all_results.extend(results)
        current_date += timedelta(days=1)

    if not all_posts:
        logger.warning(f"No posts found for Batch {batch_num}")
        return None

    metrics_map = defaultdict(dict)
    for item in all_results:
        metrics_map[item['contentId']][item['metric']] = item['value']

    export_rows = []
    today_date = date.today()
    for post in all_posts:
        c_id = post.get("id", "")
        m = metrics_map.get(c_id, {})
        ch_ids = post.get("channels", [])
        ch_uuid = ch_ids[0] if ch_ids else ""
        export_rows.append({
            "date_of_post": post.get("date", ""),
            "uuid": ch_uuid,
            "channel_name": channel_map.get(ch_uuid, ""),
            "network": post.get("network",""),
            "placement": post.get("placement",""),
            "content_id": c_id,
            "message": post.get("message", ""),
            "picture": post.get("pictures", ""),
            "likes_lifetime": m.get('content/likes/lifetime', ""),
            "video_views_lifetime": m.get('content/video_views/lifetime', ""),
            "comments_lifetime": m.get('content/comments/lifetime', ""),
            "shares_lifetime": m.get('content/shares/lifetime', ""),
            "clicks_lifetime": m.get('content/clicks/lifetime', ""),
            "engagements_lifetime": m.get('content/engagements/lifetime', ""),
            "impressions_lifetime": m.get('content/impressions/lifetime', ""),
            "reactions_lifetime": m.get('content/reactions/lifetime', ""),
            "user_follows_lifetime": m.get('content/user_follows/lifetime', ""),
            "views_lifetime": m.get('content/views/lifetime', ""),
            "saves_lifetime": m.get('content/saves/lifetime', ""),
            "profile_clicks_lifetime": m.get('content/user_profile_clicks/lifetime', ""),
            "link_clicks_lifetime": m.get('content/link_clicks/lifetime', ""),
            "reach_lifetime": m.get('content/reach/lifetime', ""),
            "frequency_lifetime": m.get('content/frequency/lifetime', ""),
            "date_of_upload": today_date,
        })

    df = pd.DataFrame(export_rows)
    df.columns = [c.replace(' ', '_').replace('/', '_') for c in df.columns]
    today_str = datetime.now().strftime('%d%m%y')
    filename = f'content_metrics_batch{batch_num}_updated_{today_str}.csv'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    full_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(full_path, index=False)
    logger.info(f"✅ Batch {batch_num} Saved: {filename} | Total Rows: {len(df)}")
    return full_path

@task(name="push_content_to_sql")
def push_content_to_sql(file_paths):
    """
    Reads CSVs, cleans data, and performs BULK INSERT into staging table.
    """
    logger = get_run_logger()
    valid_files = [f for f in file_paths if f and os.path.exists(f)]
    if not valid_files:
        logger.warning("No valid batch files to process.")
        return 0

    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};LoginTimeout=30'

    valid_columns = [
        'date_of_post', 'uuid', 'channel_name', 'network', 'placement',
        'content_id', 'message', 'picture', 'date_of_upload',
        'likes_lifetime', 'video_views_lifetime', 'comments_lifetime', 
        'shares_lifetime', 'clicks_lifetime', 'engagements_lifetime', 
        'impressions_lifetime', 'reactions_lifetime', 'user_follows_lifetime', 
        'views_lifetime', 'saves_lifetime', 'profile_clicks_lifetime', 
        'link_clicks_lifetime', 'reach_lifetime', 'frequency_lifetime'
    ]

    numeric_cols = [
        'likes_lifetime', 'video_views_lifetime', 'comments_lifetime',
        'shares_lifetime', 'clicks_lifetime', 'engagements_lifetime',
        'impressions_lifetime', 'reactions_lifetime', 'user_follows_lifetime', 
        'views_lifetime', 'saves_lifetime', 'profile_clicks_lifetime', 
        'link_clicks_lifetime', 'reach_lifetime', 'frequency_lifetime'
    ]

    dfs = []
    for fp in valid_files:
        try:
            d = pd.read_csv(fp)
            logger.info(f"📥 Reading File: {os.path.basename(fp)} | Row Count: {len(d)}")
            d.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_') for c in d.columns]
            d = d[[c for c in d.columns if c in valid_columns]]
            dfs.append(d)
        except Exception as e:
            logger.error(f"Error reading {fp}: {e}")

    if not dfs: return 0

    combined_df = pd.concat(dfs, ignore_index=True)
    initial_count = len(combined_df)
    logger.info(f"Combined Data for Staging SQL Push: {initial_count} rows total.")

    if 'date_of_post' in combined_df.columns:
        combined_df['date_of_post'] = pd.to_datetime(combined_df['date_of_post'], errors='coerce')
        combined_df = combined_df.dropna(subset=['date_of_post'])
    if 'date_of_upload' in combined_df.columns:
        combined_df['date_of_upload'] = pd.to_datetime(combined_df['date_of_upload'], errors='coerce')

    for col in numeric_cols:
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

    combined_df = combined_df.replace({np.nan: None})
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # fast_executemany disabled to prevent string truncation on picture/message columns
    
    rows_inserted = 0
    try:
        if 'date_of_upload' in combined_df.columns:
            upload_dates = combined_df['date_of_upload'].dropna().unique()
            for u_date in upload_dates:
                u_date_str = pd.to_datetime(u_date).strftime('%Y-%m-%d')
                logger.info(f"Clearing existing staging data (bwContent) for upload date: {u_date_str}")
                cursor.execute("DELETE FROM bwContent WHERE date_of_upload = ?", u_date_str)
        
        columns = ', '.join(combined_df.columns)
        placeholders = ', '.join(['?'] * len(combined_df.columns))
        sql = f"INSERT INTO bwContent ({columns}) VALUES ({placeholders})"
        data = [tuple(x if pd.notnull(x) else None for x in row) for row in combined_df.values]
        cursor.executemany(sql, data)
        conn.commit()
        
        rows_inserted = len(data)
        logger.info(f"✅ Successfully inserted {rows_inserted} rows into bwContent (Archive).")
    except Exception as e:
        logger.error(f"SQL Insert Error on staging: {e}")
        raise
    finally:
        conn.close()
    
    return rows_inserted

@task(name="calculate_and_push_deltas", retries=2, retry_delay_seconds=30)
def calculate_and_push_deltas(file_paths):
    """
    Reads the fresh CSVs, fetches historical metrics from SQL in chunks, 
    calculates daily deltas, and pushes directly to bwContent_Reporting.
    """
    logger = get_run_logger()
    logger.info("--- Starting Python Delta Calculation ---")

    valid_files = [f for f in file_paths if f and os.path.exists(f)]
    if not valid_files:
        logger.warning("No valid files found for delta calculation.")
        return 0

    dfs = []
    for fp in valid_files:
        try:
            d = pd.read_csv(fp)
            d.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_') for c in d.columns]
            dfs.append(d)
        except Exception as e:
            logger.error(f"Error reading {fp}: {e}")

    if not dfs: return 0
    current_df = pd.concat(dfs, ignore_index=True)
    
    # Clean IDs to prevent mismatches during merge
    current_df['content_id'] = current_df['content_id'].astype(str).str.strip()
    current_df = current_df.drop_duplicates(subset=['content_id', 'date_of_upload'])

    # Parse dates and strip timezones to prevent Pandas subtraction errors
    current_df['date_of_post'] = pd.to_datetime(current_df['date_of_post'], utc=True, errors='coerce').dt.tz_localize(None)
    current_df['date_of_upload'] = pd.to_datetime(current_df['date_of_upload'], utc=True, errors='coerce').dt.tz_localize(None)
    current_df = current_df.dropna(subset=['date_of_post'])
    
    # Calculate days since post
    current_df['days_since_post'] = (current_df['date_of_upload'] - current_df['date_of_post']).dt.days

    metrics = [
        'likes', 'video_views', 'comments', 'shares', 'clicks', 
        'engagements', 'impressions', 'reactions', 'user_follows', 
        'views', 'saves', 'profile_clicks', 'link_clicks', 'reach', 'frequency'
    ]

    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_ORGANICSOCIAL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};LoginTimeout=30'
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        today_str = current_df['date_of_upload'].max().strftime('%Y-%m-%d')
        content_ids = current_df['content_id'].dropna().unique().tolist()
        
        logger.info(f"Fetching previous lifetimes for {len(content_ids)} unique posts (in chunks of 500)...")

        # BULLETPROOF FETCH: Chunk the IDs to prevent 140,000+ character string limits
        prev_dfs = []
        chunk_size = 500
        for i in range(0, len(content_ids), chunk_size):
            chunk = tuple(content_ids[i:i + chunk_size])
            in_clause = f"('{chunk[0]}')" if len(chunk) == 1 else str(chunk)
            
            sql_fetch = f"""
                SELECT content_id, 
                       likes_lifetime as prev_likes, video_views_lifetime as prev_video_views,
                       comments_lifetime as prev_comments, shares_lifetime as prev_shares,
                       clicks_lifetime as prev_clicks, engagements_lifetime as prev_engagements,
                       impressions_lifetime as prev_impressions, reactions_lifetime as prev_reactions,
                       user_follows_lifetime as prev_user_follows, views_lifetime as prev_views,
                       saves_lifetime as prev_saves, profile_clicks_lifetime as prev_profile_clicks,
                       link_clicks_lifetime as prev_link_clicks, reach_lifetime as prev_reach,
                       frequency_lifetime as prev_frequency
                FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY content_id ORDER BY date_of_upload DESC) as rn
                    FROM bwContent_Reporting
                    WHERE date_of_upload < '{today_str}' 
                    AND content_id IN {in_clause}
                ) latest_prev
                WHERE rn = 1
            """
            chunk_df = pd.read_sql(sql_fetch, conn)
            prev_dfs.append(chunk_df)

        # Combine all the chunks we found
        if prev_dfs:
            prev_df = pd.concat(prev_dfs, ignore_index=True)
            logger.info(f"✅ Successfully fetched {len(prev_df)} historical records from SQL!")
        else:
            prev_df = pd.DataFrame(columns=['content_id'])
            logger.warning("⚠️ No historical records found! All previous values will default to 0.")
        
        logger.info("Calculating Daily Deltas in memory...")
        final_df = pd.merge(current_df, prev_df, on='content_id', how='left')
        
        for m in metrics:
            lt_col = f"{m}_lifetime"
            prev_col = f"prev_{m}"
            daily_col = f"daily_{m}" # Matches SQL Column schema exactly
            
            if lt_col in final_df.columns:
                final_df[lt_col] = pd.to_numeric(final_df[lt_col], errors='coerce').fillna(0)
                
                if prev_col in final_df.columns:
                    final_df[prev_col] = pd.to_numeric(final_df[prev_col], errors='coerce').fillna(0)
                else:
                    final_df[prev_col] = 0
                
                final_df[daily_col] = final_df[lt_col] - final_df[prev_col]

        # Explicitly define columns so they match bwContent_Reporting
        target_columns = [
            'date_of_post', 'uuid', 'channel_name', 'network', 'placement', 'content_id', 
            'message', 'picture', 'date_of_upload', 'days_since_post'
        ]
        for m in metrics:
            target_columns.extend([f"{m}_lifetime", f"daily_{m}"])
            
        final_df = final_df[[c for c in target_columns if c in final_df.columns]]
        final_df = final_df.replace({np.nan: None}) 
        
        logger.info(f"Clearing any existing bwContent_Reporting data for {today_str} to prevent duplicates...")
        cursor.execute("DELETE FROM bwContent_Reporting WHERE date_of_upload = ?", today_str)

        logger.info(f"Pushing {len(final_df)} processed rows to SQL Reporting Table...")
        columns_str = ', '.join(final_df.columns)
        placeholders = ', '.join(['?'] * len(final_df.columns))
        insert_sql = f"INSERT INTO bwContent_Reporting ({columns_str}) VALUES ({placeholders})"
        
        data_tuples = [tuple(x if pd.notnull(x) else None for x in row) for row in final_df.values]
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        
        rows_inserted = len(data_tuples)
        logger.info(f"✅ Successfully inserted {rows_inserted} rows into bwContent_Reporting.")
        return rows_inserted

    except Exception as e:
        logger.error(f"❌ Delta Calculation Failed: {e}")
        raise
    finally:
        conn.close()

@flow(name="Brandwatch Content Sync", log_prints=True)
def brandwatch_content_flow():
    logger = get_run_logger()
    
    try:
        channel_map = fetch_channels()
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        batches = [
            {"num": 1, "start": today - timedelta(days=90), "end": today - timedelta(days=60)},
            {"num": 2, "start": today - timedelta(days=59), "end": today - timedelta(days=30)},
            {"num": 3, "start": today - timedelta(days=29), "end": today - timedelta(days=2)}
        ]
        
        generated_files = []

        for i, batch in enumerate(batches):
            file_path = process_content_batch(batch["num"], batch["start"], batch["end"], channel_map)
            generated_files.append(file_path)
            
            # Rate Limit Logic in Flow (Best Practice)
            if i < len(batches) - 1:
                logger.info("⏳ Rate Limit Cooldown: Sleeping for 7 minutes...")
                time.sleep(420) 

        # Task 1: Insert Raw Data to Staging (Archive)
        raw_rows = push_content_to_sql(generated_files)

        # Task 2: Calculate Deltas and Update Reporting Table
        reporting_rows = 0
        if raw_rows > 0:
            reporting_rows = calculate_and_push_deltas(generated_files)

        # Success Notification
        send_teams_notification(
            f"✅ **Brandwatch Content Sync Successful**\n\n"
            f"**Date:** {date.today()}\n"
            f"**Raw Rows Archived:** {raw_rows}\n"
            f"**Reporting Rows Processed:** {reporting_rows}",
            logger
        )

    except Exception as e:
        error_message = str(e)
        send_teams_notification(
            f"❌ **Brandwatch Content Sync Failed**\n\n"
            f"**Error:** {error_message}",
            logger
        )
        raise e

if __name__ == "__main__":
    brandwatch_content_flow.serve(
        name="brandwatch-content-daily",
        cron="0 8 * * *",
        tags=["brandwatch", "content", "production"]
    )