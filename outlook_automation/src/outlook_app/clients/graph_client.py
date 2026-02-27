import msal
import requests
import time
import base64
from typing import List, Dict, Any, Tuple

class GraphClient:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, target_user: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.target_user = target_user
        self.base_url = "https://graph.microsoft.com/v1.0"
        self._token = None

    def _get_token(self) -> str:
        if not self._token:
            authority_url = f"https://login.microsoftonline.com/{self.tenant_id}"
            app = msal.ConfidentialClientApplication(
                self.client_id, authority=authority_url, client_credential=self.client_secret
            )
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            if "access_token" in result:
                self._token = result["access_token"]
            else:
                raise Exception(f"Failed to acquire Graph Token: {result.get('error_description')}")
        return self._token

    def get_headers(self) -> dict:
        return {'Authorization': f'Bearer {self._get_token()}'}

    def search_emails(self, search_query: str, top: int = 100) -> List[Dict[str, Any]]:
        """Executes a fuzzy search and handles pagination/rate limiting."""
        endpoint = f"{self.base_url}/users/{self.target_user}/messages"
        params = {'$search': search_query, '$select': 'id,subject,from,hasAttachments,receivedDateTime', '$top': top}
        
        all_emails = []
        headers = self.get_headers()
        
        while endpoint:
            resp = requests.get(endpoint, headers=headers, params=params)
            
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get('Retry-After', 10)))
                continue
                
            resp.raise_for_status()
            data = resp.json()
            all_emails.extend(data.get('value', []))
            
            endpoint = data.get('@odata.nextLink')
            params = None # Clear params as nextLink includes them
            if endpoint: time.sleep(0.5)
            
        return all_emails

    def download_attachment(self, msg_id: str, expected_ext: str) -> Tuple[bytes, str]:
        """Fetches attachments and returns the target file's bytes and its actual name."""
        endpoint = f"{self.base_url}/users/{self.target_user}/messages/{msg_id}/attachments"
        resp = requests.get(endpoint, headers=self.get_headers())
        resp.raise_for_status()
        
        attachments = resp.json().get('value', [])
        for att in attachments:
            ext = att['name'][att['name'].rfind('.'):].lower()
            if ext == expected_ext:
                return base64.b64decode(att['contentBytes']), att['name']
                
        raise ValueError(f"No attachment found with extension {expected_ext}")
