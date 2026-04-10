"""
Authentication utility for Microsoft APIs using MSAL.
"""

import msal
from config import TENANT_ID, CLIENT_ID, CLIENT_SECRET

def get_token(scopes=["https://analysis.windows.net/powerbi/api/.default"]):
    """
    Retrieves an OAuth2 access token for the specified scopes.
    """
    try:
        authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        app = msal.ConfidentialClientApplication(
            CLIENT_ID, 
            authority=authority, 
            client_credential=CLIENT_SECRET
        )
        result = app.acquire_token_for_client(scopes=scopes)
        return result.get("access_token")
    except Exception:
        return None
