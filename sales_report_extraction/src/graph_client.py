import msal
import requests
import time
import base64
from typing import List, Dict, Any, Tuple
from prefect import get_run_logger

class GraphClient:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, target_user: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.target_user = target_user
        self.base_url = "https://graph.microsoft.com/v1.0"
        self._token = None

    def _get_token(self) -> str:
        logger = get_run_logger()
        if not self._token:
            authority_url = f"https://login.microsoftonline.com/{self.tenant_id}"
            app = msal.ConfidentialClientApplication(
                self.client_id, authority=authority_url, client_credential=self.client_secret
            )
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            
            if "access_token" in result:
                self._token = result["access_token"]
                logger.debug("🔑 Successfully acquired new MSAL access token.")
            else:
                error_msg = f"Failed to acquire Graph Token: {result.get('error_description')}"
                logger.error(f"❌ {error_msg}")
                raise Exception(error_msg)
        return self._token

    def get_headers(self) -> dict:
        return {'Authorization': f'Bearer {self._get_token()}'}

    def search_emails(self, search_query: str, top: int = 100) -> List[Dict[str, Any]]:
        """Executes a fuzzy search and handles pagination/rate limiting."""
        logger = get_run_logger()
        endpoint = f"{self.base_url}/users/{self.target_user}/messages"
        # We include 'categories' in the $select so we can skip already processed emails
        params = {'$search': search_query, '$select': 'id,subject,from,hasAttachments,receivedDateTime,categories', '$top': top}
        
        all_emails = []
        headers = self.get_headers()
        
        while endpoint:
            resp = requests.get(endpoint, headers=headers, params=params)
            
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 10))
                logger.warning(f"⚠️ API Rate Limit (429) hit. Sleeping for {retry_after} seconds.")
                time.sleep(retry_after)
                continue
                
            resp.raise_for_status()
            data = resp.json()
            all_emails.extend(data.get('value', []))
            
            endpoint = data.get('@odata.nextLink')
            params = None # Clear params as nextLink includes them
            
            if endpoint: 
                logger.debug("⏭️ Paginating to next set of emails via @odata.nextLink.")
                time.sleep(0.5)
            
        if not all_emails:
            logger.info(f"📭 No emails found for query: {search_query}")
            
        return all_emails

    def download_attachment(self, msg_id: str, expected_ext: str) -> Tuple[bytes, str]:
        """Fetches attachments and returns the target file's bytes and its actual name."""
        logger = get_run_logger()
        endpoint = f"{self.base_url}/users/{self.target_user}/messages/{msg_id}/attachments"
        resp = requests.get(endpoint, headers=self.get_headers())
        resp.raise_for_status()
        
        attachments = resp.json().get('value', [])
        for att in attachments:
            ext = att['name'][att['name'].rfind('.'):].lower()
            if ext == expected_ext:
                logger.info(f"📎 Successfully downloaded expected attachment: {att['name']}")
                return base64.b64decode(att['contentBytes']), att['name']
                
        # Instead of generic failure, capture what files WERE actually there
        found_files = [a['name'] for a in attachments]
        error_msg = f"No attachment found with extension {expected_ext}. Files found: {found_files}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    def tag_email(self, msg_id: str, tag_name: str) -> bool:
        """Applies a category tag to an email in Microsoft Graph."""
        logger = get_run_logger()
        endpoint = f"{self.base_url}/users/{self.target_user}/messages/{msg_id}"
        payload = {"categories": [tag_name]}
        
        # Add a simple retry loop for Exchange server conflicts
        for attempt in range(3):
            resp = requests.patch(endpoint, headers=self.get_headers(), json=payload)
            
            if resp.status_code == 200:
                logger.info(f"🏷️ Successfully tagged email with '{tag_name}'")
                return True
                
            # If we hit an irresolvable conflict (412 or 409), sleep and try again
            if resp.status_code in [409, 412]:
                logger.warning(f"⚠️ Exchange conflict on tag attempt {attempt + 1}. Retrying...")
                time.sleep(2)
                continue
                
            logger.error(f"❌ Failed to tag email: {resp.text}")
            resp.raise_for_status()
            
        return False

    def untag_email(self, message_id: str, tag_to_remove: str):
            """Removes a specific category/tag from an email."""
            # 1. Fetch current categories
            url = f"https://graph.microsoft.com/v1.0/users/{self.target_user}/messages/{message_id}?$select=categories"
            headers = {"Authorization": f"Bearer {self._get_token()}"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            current_categories = response.json().get('categories', [])
            
            # 2. Remove the target tag if it exists
            if tag_to_remove in current_categories:
                current_categories.remove(tag_to_remove)
                
                # 3. Patch the email with the new list
                patch_url = f"https://graph.microsoft.com/v1.0/users/{self.target_user}/messages/{message_id}"
                patch_response = requests.patch(patch_url, headers=headers, json={"categories": current_categories})
                patch_response.raise_for_status()
                return True
            return False