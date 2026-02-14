import os
import requests as r
import pandas as pd
import pyodbc
import time
import numpy as np
from datetime import datetime, timedelta, date
from collections import defaultdict
from prefect import flow, task, get_run_logger

# Import Shared Utils
import brandwatch_utils as utils

# --- Configuration ---
utils.setup_environment()
API_KEY = '1SfCwWj7AAlGBPSgQFDC5Bf9PBLz6wsn' 
OUTPUT_DIR = r'C:\BrandwatchOutputs\content'
STATUSES = "published"

# --- Helper Logic ---
def fetch_posts_logic(channel_map, target_date, logger):
    since = target_date.strftime('%Y-%m-%dT00:00:00.00Z')
    until = target_date.strftime('%Y-%m-%dT23:59:59.00Z')
    url = f"{utils.BASE_URL}/publish/items"
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
    req_url = f"{utils.BASE_URL}/measure/v2/insights/content"
    headers = {"Content-Type": "application/json"}
    insight_request_ids = []
    channel_ids = list(channel_to_content.keys())
    
    # Batches of 15 channels
    for batch in [channel_ids[x:x+15] for x in range(0, len(channel_ids), 15)]:
        all_content_pairs = []
        for ch_id in batch:
            for c_id in channel_to_content[ch_id]:
                all_content_pairs.append((ch_id, c_id))
        
        # Chunks of 300 content items
        for i in range(0, len(all_content_pairs), 300):
            chunk = all_content_pairs[i:i + 300]
            payload_channels = []
            temp_map = defaultdict(list)
            for ch_id, c_id in chunk: temp_map[ch_id].append(c_id)
            for ch_id, c_ids in temp_map.items(): payload_channels.append({'id': ch_id, 'contentIds': c_ids})
            
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
            try:
                res = r.post(req_url, json=body, params={'apikey': API_KEY}, headers=headers).json()
                if res.get('insightsRequestId'): insight_request_ids.append(res.get('insightsRequestId'))
            except Exception:
                time.sleep(5)
                
    all_results = []
    pending = list(insight_request_ids)
    while pending:
        for req_id in pending[:]:
            try:
                res = r.get(f"{utils.BASE_URL}/measure/v2/insights/{req_id}", params={'apikey': API_KEY}).json()
                status = res.get('status')
                if status == 'READY':
                    data = res.get('data', {}).get('insights', {})
                    for metric, items in data.items():
                        for item in items: all_results.append({'contentId': item['contentId'], 'metric': metric, 'value': item['value']})
                    pending.remove(req_id)
                elif status == 'FAILED': pending.remove(req_id)
            except: pass
            time.sleep(0.5)
        if pending: time.sleep(2)
    return all_results

# --- Tasks ---
@task(name="fetch_channels", retries=3)
def fetch_channels():
    logger = get_run_logger()
    try:
        res = r.get(f"{utils.BASE_URL}/channels", params={'apikey': API_KEY, 'limit': 1000})
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
            if post.get("channels") and post.get("id"):
                channel_to_content[post["channels"][0]].append(post["id"])
        if channel_to_content:
            results = get_insights_logic(channel_to_content, current_date.strftime('%Y-%m-%dT00:00:00.00Z'), current_date.strftime('%Y-%m-%dT23:59:59.00Z'), logger)
            all_results.extend(results)
        current_date += timedelta(days=1)

    if not all_posts: return None

    metrics_map = defaultdict(dict)
    for item in all_results: metrics_map[item['contentId']][item['metric']] = item['value']

    export_rows = []
    today_date = date.today()
    for post in all_posts:
        c_id = post.get("id", "")
        m = metrics_map.get(c_id, {})
        export_rows.append({
            "date_of_post": post.get("date", ""),
            "uuid": post.get("channels", [""])[0],
            "channel_name": channel_map.get(post.get("channels", [""])[0], ""),
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
    filename = f'content_metrics_batch{batch_num}_updated_{datetime.now().strftime("%d%m%y")}.csv'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    full_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(full_path, index=False)
    logger.info(f"✅ Batch {batch_num} Saved: {filename} | Rows: {len(df)}")
    return full_path

@task(name="push_content_to_sql")
def push_content_to_sql(file_paths):
    logger = get_run_logger()
    valid_files = [f for f in file_paths if f and os.path.exists(f)]
    if not valid_files: return 0

    dfs = []
    for fp in valid_files:
        try:
            d = pd.read_csv(fp)
            d.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_') for c in d.columns]
            dfs.append(d)
        except Exception: pass

    if not dfs: return 0
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Cleaning
    if 'date_of_post' in combined_df.columns:
        combined_df['date_of_post'] = pd.to_datetime(combined_df['date_of_post'], errors='coerce')
        combined_df = combined_df.dropna(subset=['date_of_post'])
    
    combined_df = combined_df.replace({np.nan: None})
    
    conn = utils.get_db_connection()
    cursor = conn.cursor()
    # fast_executemany disabled for large text fields
    
    try:
        # 1. Insert Data
        if 'date_of_upload' in combined_df.columns:
            for u_date in combined_df['date_of_upload'].dropna().unique():
                cursor.execute("DELETE FROM bwContent WHERE date_of_upload = ?", pd.to_datetime(u_date).strftime('%Y-%m-%d'))
        
        placeholders = ', '.join(['?'] * len(combined_df.columns))
        sql = f"INSERT INTO bwContent ({', '.join(combined_df.columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, [tuple(x if pd.notnull(x) else None for x in row) for row in combined_df.values])
        conn.commit()
        
        # 2. Log 7-Day History (New Logic)
        try:
            logger.info("--- 📊 Verifying Upload History (Last 7 Days) ---")
            history_sql = """
                SELECT CAST(date_of_upload AS DATE) as upload_date, COUNT(*) as row_count
                FROM bwContent
                WHERE date_of_upload >= DATEADD(day, -7, GETDATE())
                GROUP BY CAST(date_of_upload AS DATE)
                ORDER BY upload_date DESC
            """
            cursor.execute(history_sql)
            rows = cursor.fetchall()

            log_msg = "\n" + "-"*35 + "\n"
            log_msg += f"{'Upload Date':<15} | {'Row Count':<10}\n"
            log_msg += "-"*35 + "\n"
            
            for row in rows:
                # row[0] is date, row[1] is count
                date_str = str(row[0])
                count_str = str(row[1])
                log_msg += f"{date_str:<15} | {count_str:<10}\n"
            
            log_msg += "-"*35
            logger.info(log_msg)
            
        except Exception as history_e:
            logger.warning(f"⚠️ Data pushed successfully, but failed to fetch history logs: {history_e}")

        return len(combined_df)
    finally:
        conn.close()

@task(name="calculate_and_push_deltas", retries=2, retry_delay_seconds=30)
def calculate_and_push_deltas(file_paths):
    logger = get_run_logger()
    valid_files = [f for f in file_paths if f and os.path.exists(f)]
    if not valid_files: return 0

    dfs = [pd.read_csv(f) for f in valid_files]
    current_df = pd.concat(dfs, ignore_index=True)
    current_df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_') for c in current_df.columns]
    
    # ID Cleaning & Dedup
    current_df['content_id'] = current_df['content_id'].astype(str).str.strip()
    current_df = current_df.drop_duplicates(subset=['content_id', 'date_of_upload'])

    # Timezone strip
    current_df['date_of_post'] = pd.to_datetime(current_df['date_of_post'], utc=True, errors='coerce').dt.tz_localize(None)
    current_df['date_of_upload'] = pd.to_datetime(current_df['date_of_upload'], utc=True, errors='coerce').dt.tz_localize(None)
    current_df['days_since_post'] = (current_df['date_of_upload'] - current_df['date_of_post']).dt.days

    metrics = ['likes', 'video_views', 'comments', 'shares', 'clicks', 'engagements', 'impressions', 'reactions', 'user_follows', 'views', 'saves', 'profile_clicks', 'link_clicks', 'reach', 'frequency']
    
    conn = utils.get_db_connection()
    cursor = conn.cursor()

    try:
        today_str = current_df['date_of_upload'].max().strftime('%Y-%m-%d')
        content_ids = current_df['content_id'].dropna().unique().tolist()
        
        # Chunked Fetch
        prev_dfs = []
        for i in range(0, len(content_ids), 500):
            chunk = tuple(content_ids[i:i + 500])
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
                    WHERE date_of_upload < '{today_str}' AND content_id IN {in_clause}
                ) latest_prev WHERE rn = 1
            """
            prev_dfs.append(pd.read_sql(sql_fetch, conn))

        prev_df = pd.concat(prev_dfs, ignore_index=True) if prev_dfs else pd.DataFrame(columns=['content_id'])
        final_df = pd.merge(current_df, prev_df, on='content_id', how='left')
        
        for m in metrics:
            lt = f"{m}_lifetime"
            prev = f"prev_{m}"
            if lt in final_df.columns:
                final_df[prev] = pd.to_numeric(final_df.get(prev, 0), errors='coerce').fillna(0)
                final_df[f"daily_{m}"] = pd.to_numeric(final_df[lt], errors='coerce').fillna(0) - final_df[prev]

        # Cleanup & Push
        final_df = final_df.replace({np.nan: None})
        cursor.execute("DELETE FROM bwContent_Reporting WHERE date_of_upload = ?", today_str)
        
        cols = [c for c in final_df.columns if c in ['date_of_post', 'uuid', 'channel_name', 'network', 'placement', 'content_id', 'message', 'picture', 'date_of_upload', 'days_since_post'] or '_lifetime' in c or 'daily_' in c]
        final_df = final_df[cols]
        
        placeholders = ', '.join(['?'] * len(final_df.columns))
        sql = f"INSERT INTO bwContent_Reporting ({', '.join(final_df.columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, [tuple(x if pd.notnull(x) else None for x in row) for row in final_df.values])
        conn.commit()
        return len(final_df)

    except Exception as e:
        logger.error(f"Delta Calculation Failed: {e}")
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
            generated_files.append(process_content_batch(batch["num"], batch["start"], batch["end"], channel_map))
            if i < len(batches) - 1:
                logger.info("⏳ Cooldown: Sleeping for 7 minutes...")
                time.sleep(420) 

        raw_rows = push_content_to_sql(generated_files)
        reporting_rows = calculate_and_push_deltas(generated_files) if raw_rows > 0 else 0

        utils.send_teams_notification(
            f"✅ **Brandwatch Content Sync Successful**\n\n**Date:** {date.today()}\n**Raw:** {raw_rows}\n**Reporting:** {reporting_rows}", logger
        )
    except Exception as e:
        utils.send_teams_notification(f"❌ **Brandwatch Content Sync Failed**\n\n**Error:** {str(e)}", logger)
        raise e

if __name__ == "__main__":
    brandwatch_content_flow.serve(name="brandwatch-content-daily", cron="30 7 * * *", tags=["brandwatch", "content"])