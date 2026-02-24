import sys
import os
import asyncio
import httpx
from pathlib import Path
import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime, timedelta, date
from collections import defaultdict
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

# 1. Best Practice: Add the parent directory to sys.path to find shared_lib
sys.path.append(str(Path(__file__).parents[1]))

# Import Shared Utils
import shared_libs.utils as utils
from shared_libs.utils import ValidationResult

# --- Configuration ---
utils.setup_environment()
API_KEY = os.getenv('BRANDWATCH_API_KEY')
OUTPUT_DIR = '/opt/data/brandwatch_outputs/content'
STATUSES = "published"

# Safely caps simultaneous HTTP requests to prevent instant 429s. Adjust as needed.
CONCURRENCY_LIMIT = 5 

# --- Async HTTP Helpers ---
async def fetch_posts_async(client, channel_map, target_date, semaphore, logger, max_retries=5):
    since = target_date.strftime('%Y-%m-%dT00:00:00.00Z')
    until = target_date.strftime('%Y-%m-%dT23:59:59.00Z')
    url = f"{utils.BASE_URL}/publish/items"
    
    retries = 0
    while retries < max_retries:
        # Reset params and list inside the loop so retries start fresh
        params = {"statuses": STATUSES, "since": since, "until": until, "limit": 100}
        all_posts = []
        
        try:
            async with semaphore:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
            raw_posts = data.get('items', [])
            next_page = data.get('paging', {}).get('nextPage')
            
            while next_page:
                params["page"] = next_page
                async with semaphore:
                    resp = await client.get(url, params=params)
                    resp.raise_for_status()
                    new_items = resp.json().get('items', [])
                    raw_posts.extend(new_items)
                    next_page = resp.json().get('paging', {}).get('nextPage')
                    
            for post in raw_posts:
                # Replicating original dark post filter logic
                if not str(post.get("facebook", {}) or post.get("instagram", {}) or post.get("tiktok", {})).startswith("{'dark': True"):
                    all_posts.append(post)
                    
            return all_posts
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 5))
                logger.warning(f"[{target_date.date()}] Rate limited on posts. Sleeping {retry_after}s (Attempt {retries + 1}/{max_retries}).")
                await asyncio.sleep(retry_after)
                retries += 1
                continue # Loops back up to try again without recursion
            logger.error(f"[{target_date.date()}] Error fetching posts: {e}")
            return []
        except Exception as e:
            logger.error(f"[{target_date.date()}] Connection error fetching posts: {e}")
            return []
            
    logger.error(f"[{target_date.date()}] Max retries ({max_retries}) exceeded for fetching posts.")
    return []

async def request_insight_id(client, payload, semaphore, logger, max_retries=5):
    req_url = f"{utils.BASE_URL}/measure/v2/insights/content"
    
    retries = 0
    while retries < max_retries:
        try:
            # We grab the semaphore inside the loop so we don't hog the concurrency slot while sleeping
            async with semaphore:
                response = await client.post(req_url, json=payload)
                response.raise_for_status()
                return response.json().get('insightsRequestId')
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 5))
                logger.warning(f"Rate limited on report submit. Sleeping {retry_after}s (Attempt {retries + 1}/{max_retries}).")
                await asyncio.sleep(retry_after)
                retries += 1
                continue
            else:
                logger.error(f"HTTP error on insight submit: {e}")
                return None
        except Exception as e:
            logger.error(f"Error requesting insight ID: {e}")
            return None
            
    logger.error(f"Max retries ({max_retries}) exceeded for requesting insight ID.")
    return None

async def poll_insight_data(client, req_id, semaphore, logger):
    poll_url = f"{utils.BASE_URL}/measure/v2/insights/{req_id}"
    while True:
        async with semaphore:
            try:
                response = await client.get(poll_url)
                response.raise_for_status()
                res_json = response.json()
                status = res_json.get('status')
                
                if status == 'READY':
                    return res_json.get('data', {}).get('insights', {})
                elif status == 'FAILED':
                    logger.warning(f"Insight request {req_id} FAILED on server.")
                    return None
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 3))
                    await asyncio.sleep(retry_after)
                    continue
                logger.error(f"HTTP error polling {req_id}: {e}")
                return None
            except Exception as e:
                logger.error(f"Error polling {req_id}: {e}")
                return None
        
        await asyncio.sleep(2) # Cooldown before polling same ID again

async def process_day_async(client, target_date, channel_map, semaphore, logger):
    """Fetches posts and their insights for a single day concurrently."""
    posts = await fetch_posts_async(client, channel_map, target_date, semaphore, logger)
    if not posts:
        return [], []

    channel_to_content = defaultdict(list)
    for post in posts:
        if post.get("channels") and post.get("id"):
            channel_to_content[post["channels"][0]].append(post["id"])
            
    if not channel_to_content:
        return posts, []

    # Prepare Insight Payloads
    channel_ids = list(channel_to_content.keys())
    payloads = []
    since_req = target_date.strftime('%Y-%m-%dT00:00:00.00Z')
    until_req = target_date.strftime('%Y-%m-%dT23:59:59.00Z')
    
    for batch in [channel_ids[x:x+15] for x in range(0, len(channel_ids), 15)]:
        all_content_pairs = [(ch_id, c_id) for ch_id in batch for c_id in channel_to_content[ch_id]]
        for i in range(0, len(all_content_pairs), 300):
            chunk = all_content_pairs[i:i + 300]
            temp_map = defaultdict(list)
            for ch_id, c_id in chunk: temp_map[ch_id].append(c_id)
            
            payload_channels = [{'id': k, 'contentIds': v} for k, v in temp_map.items()]
            payloads.append({
                'since': since_req, 'until': until_req,
                'metricIds': [
                    'content/likes/lifetime', 'content/video_views/lifetime', 'content/comments/lifetime',
                    'content/shares/lifetime', 'content/clicks/lifetime', 'content/engagements/lifetime',
                    'content/impressions/lifetime', 'content/reactions/lifetime', 'content/user_follows/lifetime',
                    'content/views/lifetime', 'content/saves/lifetime', 'content/user_profile_clicks/lifetime',
                    'content/link_clicks/lifetime', 'content/reach/lifetime', 'content/frequency/lifetime'
                ],
                'channels': payload_channels
            })

    # Submit and Poll Insights
    submit_tasks = [request_insight_id(client, p, semaphore, logger) for p in payloads]
    req_ids = await asyncio.gather(*submit_tasks)
    valid_req_ids = [rid for rid in req_ids if rid]
    
    poll_tasks = [poll_insight_data(client, rid, semaphore, logger) for rid in valid_req_ids]
    raw_insights = await asyncio.gather(*poll_tasks)

    # Flatten Results
    all_results = []
    for data in raw_insights:
        if not data: continue
        for metric, items in data.items():
            for item in items:
                all_results.append({'contentId': item['contentId'], 'metric': metric, 'value': item['value']})
                
    return posts, all_results


# --- Prefect Tasks ---
@task(name="fetch_channels", retries=3)
async def fetch_channels():
    logger = get_run_logger()
    logger.info("Fetching channel list from Brandwatch...")
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(f"{utils.BASE_URL}/channels", params={'apikey': API_KEY, 'limit': 1000})
            res.raise_for_status()
            items = res.json().get('items', [])
            logger.info(f"Successfully mapped {len(items)} channels.")
            return {ch['uuid']: ch.get('name', 'No name') for ch in items}
    except Exception as e:
        logger.error(f"Failed to fetch channels: {e}")
        raise

@task(name="process_content_batch", retries=2)
async def process_content_batch(batch_num, start_date, end_date, channel_map):
    logger = get_run_logger()
    logger.info(f"========== Starting Batch {batch_num} ==========")
    logger.info(f"Date Range: {start_date.date()} to {end_date.date()}")
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    headers = {"Content-Type": "application/json"}
    
    # Generate list of dates
    num_days = (end_date - start_date).days + 1
    date_list = [start_date + timedelta(days=x) for x in range(num_days)]
    
    all_posts = []
    all_results = []
    
    # Execute all days concurrently
    logger.info(f"Initiating concurrent fetch for {num_days} days...")
    async with httpx.AsyncClient(params={'apikey': API_KEY}, headers=headers, timeout=30.0) as client:
        tasks = [process_day_async(client, d, channel_map, semaphore, logger) for d in date_list]
        day_outputs = await asyncio.gather(*tasks)
        
    for posts, insights in day_outputs:
        all_posts.extend(posts)
        all_results.extend(insights)

    if not all_posts: 
        logger.warning(f"No posts found for Batch {batch_num}.")
        return None

    logger.info(f"Fetched {len(all_posts)} posts and {len(all_results)} insight metrics. Compiling dataframes...")
    
    # Vectorized DataFrame Assembly
    df_posts = pd.DataFrame([{
        "date_of_post": p.get("date", ""),
        "uuid": p.get("channels", [""])[0] if p.get("channels") else "",
        "channel_name": channel_map.get(p.get("channels", [""])[0] if p.get("channels") else "", ""),
        "network": p.get("network",""),
        "placement": p.get("placement",""),
        "content_id": p.get("id", ""),
        "message": p.get("message", ""),
        "picture": p.get("pictures", ""),
        "date_of_upload": date.today()
    } for p in all_posts])

    if all_results:
        # Pivot insights and merge
        df_insights = pd.DataFrame(all_results)
        df_insights = df_insights.pivot(index='contentId', columns='metric', values='value').reset_index()
        # Clean column names
        df_insights.columns = [c.replace('content/', '').replace('/', '_') if c != 'contentId' else c for c in df_insights.columns]
        
        # FIX: Rename column to match original SQL schema
        if 'user_profile_clicks_lifetime' in df_insights.columns:
            df_insights = df_insights.rename(columns={'user_profile_clicks_lifetime': 'profile_clicks_lifetime'})
        
        # Merge posts and metrics on content ID
        df_final = pd.merge(df_posts, df_insights, left_on='content_id', right_on='contentId', how='left').drop(columns=['contentId'])
    else:
        df_final = df_posts

    # Standardize column formats
    df_final.columns = [c.replace(' ', '_').replace('/', '_') for c in df_final.columns]
    
    filename = f'content_metrics_batch{batch_num}_updated_{datetime.now().strftime("%d%m%y")}.csv'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    full_path = os.path.join(OUTPUT_DIR, filename)
    df_final.to_csv(full_path, index=False)
    
    logger.info(f"✅ Batch {batch_num} Complete! Saved to {filename} | Total Rows: {len(df_final)}")
    return full_path

@task(name="push_content_to_sql", retries=3, retry_delay_seconds=60)
def push_content_to_sql(file_paths):
    logger = get_run_logger()
    valid_files = [f for f in file_paths if f and os.path.exists(f)]
    if not valid_files: 
        logger.warning("No valid files to push to SQL.")
        return 0

    logger.info(f"Reading {len(valid_files)} batch files for DB ingestion...")
    dfs = []
    for fp in valid_files:
        try:
            d = pd.read_csv(fp)
            d.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_') for c in d.columns]
            dfs.append(d)
        except Exception as e: 
            logger.error(f"Failed to read file {fp}: {e}")

    if not dfs: return 0
    combined_df = pd.concat(dfs, ignore_index=True)
    
    if 'date_of_post' in combined_df.columns:
        combined_df['date_of_post'] = pd.to_datetime(combined_df['date_of_post'], errors='coerce')
        combined_df = combined_df.dropna(subset=['date_of_post'])
    
    combined_df = combined_df.replace({np.nan: None})
    
    logger.info("Connecting to SQL Database (bwContent)...")
    conn = utils.get_db_connection()
    cursor = conn.cursor()
    
    # TIP: If you have updated your SQL VARCHAR sizes to MAX, you can add `cursor.fast_executemany = True` here for a 10x speed boost.
    
    try:
        if 'date_of_upload' in combined_df.columns:
            for u_date in combined_df['date_of_upload'].dropna().unique():
                logger.info(f"Clearing existing records for upload date: {pd.to_datetime(u_date).strftime('%Y-%m-%d')}")
                cursor.execute("DELETE FROM bwContent WHERE date_of_upload = ?", pd.to_datetime(u_date).strftime('%Y-%m-%d'))
        
        logger.info("Executing database inserts...")
        placeholders = ', '.join(['?'] * len(combined_df.columns))
        sql = f"INSERT INTO bwContent ({', '.join(combined_df.columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, [tuple(x if pd.notnull(x) else None for x in row) for row in combined_df.values])
        conn.commit()
        
        logger.info(f"✅ Successfully inserted {len(combined_df)} raw rows into bwContent.")
        return len(combined_df)
    finally:
        conn.close()

@task(name="calculate_and_push_deltas", retries=2, retry_delay_seconds=30)
def calculate_and_push_deltas(file_paths):
    logger = get_run_logger()
    logger.info("========== Starting Delta Calculation ==========")
    valid_files = [f for f in file_paths if f and os.path.exists(f)]
    if not valid_files: 
        return 0, ValidationResult("FAILED", "No valid files provided for delta calculation.", {})

    dfs = [pd.read_csv(f) for f in valid_files]
    current_df = pd.concat(dfs, ignore_index=True)
    current_df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_') for c in current_df.columns]
    
    current_df['content_id'] = current_df['content_id'].astype(str).str.strip()
    original_len = len(current_df)
    current_df = current_df.drop_duplicates(subset=['content_id', 'date_of_upload'])
    logger.info(f"Deduplicated base data. Rows dropped: {original_len - len(current_df)}")

    current_df['date_of_post'] = pd.to_datetime(current_df['date_of_post'], utc=True, errors='coerce').dt.tz_localize(None)
    current_df['date_of_upload'] = pd.to_datetime(current_df['date_of_upload'], utc=True, errors='coerce').dt.tz_localize(None)
    current_df['days_since_post'] = (current_df['date_of_upload'] - current_df['date_of_post']).dt.days

    metrics = ['likes', 'video_views', 'comments', 'shares', 'clicks', 'engagements', 'impressions', 'reactions', 'user_follows', 'views', 'saves', 'profile_clicks', 'link_clicks', 'reach', 'frequency']
    
    conn = utils.get_db_connection()
    cursor = conn.cursor()

    try:
        today_str = current_df['date_of_upload'].max().strftime('%Y-%m-%d')
        content_ids = current_df['content_id'].dropna().unique().tolist()
        logger.info(f"Fetching historical data for {len(content_ids)} unique content items...")
        
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
        logger.info("Merging current data with historical records...")
        final_df = pd.merge(current_df, prev_df, on='content_id', how='left')
        
        logger.info("Calculating daily metric deltas...")
        for m in metrics:
            lt = f"{m}_lifetime"
            prev = f"prev_{m}"
            if lt in final_df.columns:
                final_df[prev] = pd.to_numeric(final_df.get(prev, 0), errors='coerce').fillna(0)
                final_df[f"daily_{m}"] = pd.to_numeric(final_df[lt], errors='coerce').fillna(0) - final_df[prev]

        final_df = final_df.replace({np.nan: None})
        logger.info(f"Clearing today's existing reporting data ({today_str})...")
        cursor.execute("DELETE FROM bwContent_Reporting WHERE date_of_upload = ?", today_str)
        
        cols = [c for c in final_df.columns if c in ['date_of_post', 'uuid', 'channel_name', 'network', 'placement', 'content_id', 'message', 'picture', 'date_of_upload', 'days_since_post'] or '_lifetime' in c or 'daily_' in c]
        final_df = final_df[cols]
        
        logger.info("Pushing delta calculations to bwContent_Reporting...")
        placeholders = ', '.join(['?'] * len(final_df.columns))
        sql = f"INSERT INTO bwContent_Reporting ({', '.join(final_df.columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, [tuple(x if pd.notnull(x) else None for x in row) for row in final_df.values])
        conn.commit()
        
        logger.info(f"✅ Successfully inserted {len(final_df)} delta rows into reporting table.")
        status = "PASSED" if len(final_df) > 0 else "UNVALIDATED"
        message = "✅ Deltas updated." if len(final_df) > 0 else "⚠️ No new content rows processed."
        
        # Updated Artifact Key to reflect database table name
        val_result = ValidationResult(status=status, message=message, metrics={"bwContent_Reporting Rows": len(final_df)})
        return len(final_df), val_result

    except Exception as e:
        logger.error(f"Delta Calculation Failed: {e}")
        return 0, ValidationResult("FAILED", f"Error calculating deltas: {e}", {})
    finally:
        conn.close()

@flow(name="Brandwatch Content Sync", log_prints=True)
async def brandwatch_content_flow():
    logger = get_run_logger()
    logger.info("🚀 Booting Brandwatch Content Sync Pipeline (Async Mode)")
    try:
        channel_map = await fetch_channels()
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Batch Windows (Last 90 days split into ~30 day chunks)
        batches = [
            {"num": 1, "start": today - timedelta(days=90), "end": today - timedelta(days=60)},
            {"num": 2, "start": today - timedelta(days=59), "end": today - timedelta(days=30)},
            {"num": 3, "start": today - timedelta(days=29), "end": today - timedelta(days=2)}
        ]
        
        generated_files = []
        for batch in batches:
            result = await process_content_batch(batch["num"], batch["start"], batch["end"], channel_map)
            generated_files.append(result)

        logger.info("========== Proceeding to Database Operations ==========")
        raw_rows = push_content_to_sql(generated_files)
        
        if raw_rows > 0:
            reporting_rows, val_result = calculate_and_push_deltas(generated_files)
            # Updated Artifact Key to reflect database table name
            val_result.metrics["bwContent Rows"] = raw_rows
        else:
            val_result = ValidationResult("UNVALIDATED", "⚠️ 0 raw rows fetched, delta calculation skipped.", {"bwContent Rows": 0, "bwContent_Reporting Rows": 0})

        if val_result.status == "FAILED":
            raise ValueError(f"Data Validation Failed: {val_result.message}")

        # Artifact Generation
        logger.info("Generating Markdown Artifacts...")
        md_table = f"## Validation Result: {val_result.status}\n\n**Message:** {val_result.message}\n\n| Metric | Value |\n|---|---|\n"
        for key, val in val_result.metrics.items(): md_table += f"| {key} | {val} |\n"
        create_markdown_artifact(key="brandwatch-content-val", markdown=md_table, description="Content Sync Validation")

        if val_result.status == "UNVALIDATED":
            utils.send_teams_notification(f"⚠️ **Brandwatch Content: Manual Review**\n\n{val_result.message}", logger)
        else:
            logger.info("✅ Teams Alert Bypassed: Validation PASSED natively.")
            
        logger.info("🎉 Brandwatch Content Sync Pipeline finished successfully.")

    except Exception as e:
        logger.error("🚨 Critical Error in Pipeline.")
        utils.send_teams_notification(f"❌ **Brandwatch Content Sync Failed**\n\n**Error:** {str(e)}", logger)
        raise e

if __name__ == "__main__":
    asyncio.run(brandwatch_content_flow.serve(name="brandwatch-content-daily", cron="30 7 * * *", tags=["brandwatch", "content"]))