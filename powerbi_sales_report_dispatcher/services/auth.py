"""
services/auth.py — Azure AD tokens.

One app registration covers both APIs; the scope decides which token you get.
'/.default' means 'all the permissions granted to this app registration'.
"""

import msal

from config import TENANT_ID, CLIENT_ID, CLIENT_SECRET

POWERBI_SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]
GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]


class LiveReportingEngine:
    def __init__(self):
        self.authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        self.msal_app = msal.ConfidentialClientApplication(CLIENT_ID, authority=self.authority, client_credential=CLIENT_SECRET)

    def get_token(self, scopes):
        """Returns None rather than raising if Azure AD refuses — callers check."""
        return self.msal_app.acquire_token_for_client(scopes=scopes).get("access_token")
