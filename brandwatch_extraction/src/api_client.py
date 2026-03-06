import time
import requests
from prefect import get_run_logger
from .database import insert_raw_json

BASE_URL = "https://api.falcon.io"

class BrandwatchClient:
    def __init__(self, api_keys):
        self.api_keys = api_keys

    def _get_key(self):
        # Rotate keys to manage rate limits
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
                if attempt == retries - 1: raise e
                time.sleep(10)

    def poll_insight(self, req_id, tag, prefix="/measure/v2/insights"):
        logger = get_run_logger()
        logger.info(f"Polling {tag} request {req_id}...")
        while True:
            res = self.call('GET', f"{prefix}/{req_id}")
            status = res.get('status')
            if status in ['READY', 'COMPLETED']:
                if tag != 'ENGAGE_EXPORTS':
                    insert_raw_json(tag, res)
                return res.get('url') if tag == 'ENGAGE_EXPORTS' else True
            elif status == 'FAILED':
                raise Exception(f"Brandwatch processing failed for {tag}")
            time.sleep(20)
