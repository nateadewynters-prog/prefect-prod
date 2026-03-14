import os
import requests
import msal
import urllib.parse
from prefect import get_run_logger
from src.notifications import send_teams_notification

class SharePointUploader:
    """
    Dedicated client for uploading Medallion files to the 'SALES REPORTING' SharePoint Site.
    """
    def __init__(self):
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.site_id = os.getenv("SHAREPOINT_SALES_REPORTING_SITE_ID")
        
        if not all([self.tenant_id, self.client_id, self.client_secret, self.site_id]):
            raise ValueError("Missing Azure credentials or SHAREPOINT_SALES_REPORTING_SITE_ID in .env")
            
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret
        )

    def _get_token(self):
        result = self.app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if "access_token" in result:
            return result["access_token"]
        raise Exception(f"Failed to authenticate: {result.get('error_description')}")

    def _sanitize_name(self, name):
        """Removes illegal SharePoint characters from Show or Venue names."""
        invalid_chars = ['"', '*', ':', '<', '>', '?', '/', '\\', '|']
        for char in invalid_chars:
            name = name.replace(char, '-')
        return name.strip()

    def upload_file(self, local_file_path, filename, show_name, venue_name, folder_type):
        """
        Uploads to: Root / {Show} / {Venue} / {Raw|Processed} / {filename}
        Returns the SharePoint Web URL if successful, or False if it failed.
        """
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        clean_show = self._sanitize_name(show_name)
        clean_venue = self._sanitize_name(venue_name)
        
        target_folder = f"{clean_show}/{clean_venue}/{folder_type}"
        
        encoded_folder = urllib.parse.quote(target_folder)
        encoded_file = urllib.parse.quote(filename)

        logger.info(f"☁️ Uploading {filename} to SharePoint: '{target_folder}'...")

        try:
            token = self._get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream"
            }

            url = f"{self.base_url}/sites/{self.site_id}/drive/root:/{encoded_folder}/{encoded_file}:/content"

            with open(local_file_path, 'rb') as f:
                file_data = f.read()

            response = requests.put(url, headers=headers, data=file_data)

            if response.status_code in (200, 201):
                logger.info(f"✅ SharePoint upload successful! ({folder_type})")
                # 🚀 RETURN THE ACTUAL LINK SO WE CAN USE IT IN TEAMS
                return response.json().get("webUrl")
            else:
                logger.error(f"❌ SharePoint upload failed: {response.text}")
                send_teams_notification(
                    message="⚠️ **SharePoint Upload Failed**", 
                    logger=logger,
                    facts={"File": filename, "Folder": target_folder, "Error": str(response.status_code)}
                )
                return False

        except Exception as e:
            logger.error(f"❌ SharePoint connection error: {e}")
            return False
