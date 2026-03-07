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

# 3. DYNAMICALLY LOAD API KEYS
API_KEYS = [
    os.getenv(k) for k in os.environ 
    if k.startswith('BRANDWATCH_API_KEY')
]

if not API_KEYS:
    raise ValueError("No Brandwatch API keys found! Check /opt/prefect/prod/.env")

# Initialize Client
client = BrandwatchClient(API_KEYS)

@task(name="Stage Raw JSON", retries=3, retry_delay_seconds=30)
def stage_data(endpoint_tag, raw_data):
    """
    Wraps the database insertion in a Prefect task to handle 
    transient connection issues (like HYT00 timeouts).
    """
    insert_raw_json(endpoint_tag, raw_data)

@flow(name="Brandwatch Social Extraction", log_prints=True)
def brandwatch_flow():
    logger = get_run_logger()
    logger.info("🚀 Starting Brandwatch Modular ELT Pipeline")

    # --- Time Windows ---
    today = datetime.now(timezone.utc)
    target_date = today - timedelta(days=2)  # The "Settled" data day
    long_start = today - timedelta(days=82)  # Start of 90-day post sweep
    content_end = today - timedelta(days=2)

    # --- 1. Sync Master Channel List ---
    logger.info("📡 Syncing Master Channels...")
    channels_res = client.call('GET', '/channels')
    channels = channels_res.get('items', [])
    ch_uuids = [c['uuid'] for c in channels]
    stage_data('CHANNELS', channels)

    # --- 2. Section A: Post Metrics (90-Day Sweep) ---
    curr = long_start
    while curr <= content_end:
        nxt = min(curr + timedelta(days=10), content_end)
        s_iso = curr.strftime('%Y-%m-%dT00:00:00.000Z')
        u_iso = nxt.strftime('%Y-%m-%dT23:59:59.999Z')
        
        logger.info(f"⏳ Processing Posts: {s_iso[:10]} to {u_iso[:10]}")
        posts_res = client.call('GET', '/publish/items', {'since': s_iso, 'until': u_iso, 'limit': 100})
        posts = posts_res.get('items', [])
        
        if posts:
            stage_data('POST_META', posts_res)
            
            # Map posts to channels for batching
            c_to_p = {}
            for p in posts:
                if 'id' in p and p.get('channels'):
                    ch = p['channels'][0]
                    c_to_p.setdefault(ch, []).append(p['id'])
            
            # Create payload (max 300 IDs per channel)
            content_payload = []
            for ch, ids in c_to_p.items():
                for k in range(0, len(ids), 300):
                    content_payload.append({"id": ch, "contentIds": ids[k:k+300]})

            # Request metrics in batches of 15
            for j in range(0, len(content_payload), 15):
                batch = content_payload[j:j+15]
                co_req = client.call('POST', '/measure/v2/insights/content', json_data={
                    'since': s_iso, 'until': u_iso, 'metricIds': CONTENT_METRICS, 'channels': batch
                })
                client.poll_insight(co_req['insightsRequestId'], 'POST_METRICS')
        
        curr = nxt + timedelta(days=1)

    # --- 3. Section B: Daily Growth & Comments (Settled Data) ---
    s_sh = target_date.strftime('%Y-%m-%d')
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
        'startDate': target_date.strftime('%Y-%m-%dT00:00:00.000Z'),
        'endDate': target_date.strftime('%Y-%m-%dT23:59:59.999Z'),
        'networks': ['facebook', 'instagram', 'twitter'],
        'types': ['comment']
    })
    csv_url = client.poll_insight(eng_req['uuid'], 'ENGAGE_EXPORTS', prefix='/engage/v2/exports')
    
    if csv_url:
        logger.info("📑 Ingesting Engage Comments...")
        res = requests.get(csv_url)
        rows = list(csv.DictReader(io.StringIO(res.text)))
        for k in range(0, len(rows), 500):
            stage_data('ENGAGE_EXPORTS', rows[k:k+500])

    logger.info("✅ Brandwatch Pipeline Finished Successfully.")

if __name__ == "__main__":
    # Serves the flow to the Prefect Dashboard with a 1:00 PM schedule
    brandwatch_flow.serve(
        name="brandwatch-daily-sync",
        cron="0 13 * * *",
        tags=["production", "social-media"]
    )
