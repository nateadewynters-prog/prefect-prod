import os
import requests
import csv
import io
from datetime import datetime, timedelta, timezone
from prefect import flow, task, get_run_logger

# 1. LOAD ENVIRONMENT FIRST
from src.env_setup import setup_environment
setup_environment()

# 2. IMPORT MODULAR MUSCLES
from src.api_client import BrandwatchClient
from src.database import insert_raw_json
from src.constants import CONTENT_METRICS, CHANNEL_METRICS
from src.notifications import send_teams_notification # 🚀 THE FIX: Import your standardized module

# 3. DYNAMICALLY LOAD API KEYS
API_KEYS = [
    os.getenv(k) for k in os.environ 
    if k.startswith('BRANDWATCH_API_KEY')
]

if not API_KEYS:
    error_msg = "No Brandwatch API keys found! Check /opt/prefect/prod/.env"
    send_teams_notification(f"🚨 **Brandwatch Config Error**\n\n{error_msg}") # 🚀 Alert if config is missing
    raise ValueError(error_msg)

# Initialize Client
client = BrandwatchClient(API_KEYS)

@task(name="Stage Raw JSON", retries=3, retry_delay_seconds=30)
def stage_data(endpoint_tag, raw_data):
    """Wraps the database insertion in a Prefect task to handle transient connection issues."""
    insert_raw_json(endpoint_tag, raw_data)

@task(name="Sync Master Channels", retries=2)
def sync_channels():
    logger = get_run_logger()
    logger.info("📡 Syncing Master Channels...")
    channels_res = client.call('GET', '/channels')
    channels = channels_res.get('items', [])
    stage_data('CHANNELS', channels)
    return [c['uuid'] for c in channels]

@task(name="Sync Post Metrics (90-Day Sweep)", retries=2)
def sync_post_metrics(start_dt, end_dt):
    logger = get_run_logger()
    curr = start_dt
    
    while curr <= end_dt:
        nxt = min(curr + timedelta(days=10), end_dt)
        s_iso = curr.strftime('%Y-%m-%dT00:00:00.000Z')
        u_iso = nxt.strftime('%Y-%m-%dT23:59:59.999Z')
        
        logger.info(f"⏳ Processing Posts: {s_iso[:10]} to {u_iso[:10]}")
        posts_res = client.call('GET', '/publish/items', {'since': s_iso, 'until': u_iso, 'limit': 100})
        posts = posts_res.get('items', [])
        
        if posts:
            stage_data('POST_META', posts_res)
            
            c_to_p = {}
            for p in posts:
                if 'id' in p and p.get('channels'):
                    ch = p['channels'][0]
                    c_to_p.setdefault(ch, []).append(p['id'])
            
            content_payload = []
            for ch, ids in c_to_p.items():
                for k in range(0, len(ids), 300):
                    content_payload.append({"id": ch, "contentIds": ids[k:k+300]})

            for j in range(0, len(content_payload), 15):
                batch = content_payload[j:j+15]
                co_req = client.call('POST', '/measure/v2/insights/content', json_data={
                    'since': s_iso, 'until': u_iso, 'metricIds': CONTENT_METRICS, 'channels': batch
                })
                client.poll_insight(co_req['insightsRequestId'], 'POST_METRICS')
        curr = nxt + timedelta(days=1)

@task(name="Sync Settled Data (Channels & Comments)", retries=2)
def sync_settled_data(target_dt, ch_uuids):
    logger = get_run_logger()
    s_sh = target_dt.strftime('%Y-%m-%d')
    logger.info(f"📤 Syncing Settled Data for: {s_sh}")

    # Channel Metrics Batching
    for j in range(0, len(ch_uuids), 15):
        ch_batch = ch_uuids[j:j+15]
        ch_req = client.call('POST', '/measure/v2/insights/channel', json_data={
            'since': s_sh, 'until': s_sh, 'metricIds': CHANNEL_METRICS, 'channelIds': ch_batch
        })
        client.poll_insight(ch_req['insightsRequestId'], 'CH_METRICS')

    # Engage Comments Export
    eng_req = client.call('POST', '/engage/v2/exports', json_data={
        'startDate': target_dt.strftime('%Y-%m-%dT00:00:00.000Z'),
        'endDate': target_dt.strftime('%Y-%m-%dT23:59:59.999Z'),
        'networks': ['facebook', 'instagram', 'twitter'],
        'types': ['comment']
    })
    csv_url = client.poll_insight(eng_req['uuid'], 'ENGAGE_EXPORTS', prefix='/engage/v2/exports')
    
    if csv_url:
        logger.info("📑 Streaming Engage Comments to Database...")
        # 🚀 THE FIX: stream=True prevents loading the whole file into RAM
        with requests.get(csv_url, stream=True) as res:
            res.raise_for_status()
            
            # Use iter_lines() to read the stream chunk by chunk
            lines = (line.decode('utf-8') for line in res.iter_lines() if line)
            reader = csv.DictReader(lines)
            
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= 500:
                    stage_data('ENGAGE_EXPORTS', batch)
                    batch = [] # Clear memory
                    
            if batch: # Catch the remainder
                stage_data('ENGAGE_EXPORTS', batch)


@flow(name="Brandwatch Social Extraction", log_prints=True)
def brandwatch_flow():
    logger = get_run_logger()
    logger.info("🚀 Starting Brandwatch Modular ELT Pipeline")

    # --- Time Windows ---
    today = datetime.now(timezone.utc)
    target_date = today - timedelta(days=2)  
    long_start = today - timedelta(days=82)  
    content_end = today - timedelta(days=2)

    # 🚀 THE FIX: Wrap execution in a try/except block to catch terminal failures
    try:
        # --- Execute Tasks ---
        ch_uuids = sync_channels()
        sync_post_metrics(long_start, content_end)
        sync_settled_data(target_date, ch_uuids)

        logger.info("✅ Brandwatch Pipeline Finished Successfully.")
        
    except Exception as e:
        # Fires the standard Adaptive Card with a direct link to the Prefect UI
        send_teams_notification(f"❌ **Brandwatch Pipeline Failed**\n\n**Error:** {str(e)}", logger)
        raise # Ensure Prefect still registers this flow run as FAILED

if __name__ == "__main__":
    brandwatch_flow.serve(
        name="brandwatch-daily-sync",
        cron="0 13 * * *",
        tags=["production", "social-media"],
        limit=1 
    )
