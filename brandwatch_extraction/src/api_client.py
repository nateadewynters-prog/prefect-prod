import time
import requests
from prefect import get_run_logger
from .database import insert_raw_json
from src.notifications import send_teams_notification

BASE_URL = "https://api.falcon.io"

class BrandwatchClient:
    def __init__(self, api_keys):
        self.api_keys = api_keys

    def _get_key(self):
        key = self.api_keys.pop(0)
        self.api_keys.append(key)
        return key

    def call(self, method, endpoint, params=None, json_data=None, retries=5):
        logger = get_run_logger()
        params = params or {}
        
        for attempt in range(retries):
            params['apikey'] = self._get_key()
            try:
                res = requests.request(method, f"{BASE_URL}{endpoint}", params=params, json=json_data, timeout=30)
                if res.status_code in [500, 502, 503, 504]:
                    time.sleep(20)
                    continue
                res.raise_for_status()
                return res.json()
                
            except Exception as e:
                if attempt == retries - 1: 
                    error_msg = f"API Call Failed after {retries} attempts to {endpoint}: {e}"
                    logger.error(f"❌ {error_msg}")
                    send_teams_notification(
                        message="🚨 **Brandwatch API Error**", 
                        logger=logger,
                        facts={"Endpoint": endpoint, "Attempts": retries, "Error": str(e)},
                        channel="dev"
                    )
                    raise e

    def poll_insight(self, req_id, tag, prefix="/measure/v2/insights"):
        logger = get_run_logger()
        logger.info(f"Polling {tag} request {req_id}...")
        
        max_attempts = 90  
        attempts = 0
        
        while attempts < max_attempts:
            res = self.call('GET', f"{prefix}/{req_id}")
            status = res.get('status')

            if status in ['READY', 'COMPLETED']:
                if tag != 'ENGAGE_EXPORTS':
                    insert_raw_json(tag, res)
                return res.get('url') if tag == 'ENGAGE_EXPORTS' else True

            elif status == 'FAILED':
                error_msg = f"Brandwatch processing failed for {tag} request {req_id}"
                logger.error(f"❌ {error_msg}")
                send_teams_notification(
                    message="🚨 **Brandwatch Async Job Failed**", 
                    logger=logger,
                    facts={"Tag": tag, "Request ID": req_id, "Backend Status": "FAILED"},
                    channel="dev"
                )
                raise Exception(error_msg)

            time.sleep(20)
            attempts += 1
            
        timeout_msg = f"Backend processing for {tag} request {req_id} timed out after 30 minutes. Aborting to prevent Zombie Run."
        logger.error(f"❌ {timeout_msg}")
        send_teams_notification(
            message="🚨 **Brandwatch API Timeout**", 
            logger=logger,
            facts={"Tag": tag, "Request ID": req_id, "Duration": "> 30 Minutes"},
            channel="dev"
        )
        raise TimeoutError(timeout_msg)