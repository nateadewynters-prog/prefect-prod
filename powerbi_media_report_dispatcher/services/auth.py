"""
services/auth.py — Azure AD authentication (MSAL client-credentials flow).

One app registration covers both APIs we call; you just ask for a token with
the right scope. The two scopes we ever use are defined here so callers
don't have to remember the URLs.
"""

import msal

from config import TENANT_ID, CLIENT_ID, CLIENT_SECRET

# "/.default" means "all the permissions granted to this app registration".
POWERBI_SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]
GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]


def get_token(scopes: list) -> str | None:
    """Fetch an access token, or None if auth failed (bad credentials etc.)."""
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app_ = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority,
                                              client_credential=CLIENT_SECRET)
    return app_.acquire_token_for_client(scopes=scopes).get("access_token")
