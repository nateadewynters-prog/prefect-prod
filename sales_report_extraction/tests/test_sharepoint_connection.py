import os
import requests
import msal
import pytest
from src.env_setup import setup_environment

def test_live_sharepoint_connection():
    """
    LIVE INTEGRATION TEST: Verifies real connection to the Dewynters SharePoint site.
    Requires valid Azure credentials in your .env file.
    """
    setup_environment()
    
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    
    if not (tenant_id and client_id and client_secret):
        pytest.fail("❌ Missing Azure credentials in .env!")
    
    print("\n🔐 Authenticating with Azure Active Directory...")
    
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )
    
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    
    # Expose the EXACT authentication payload if it fails
    if "access_token" not in result:
        print(f"\n❌ RAW MSAL AUTH ERROR:\n{result}")
        pytest.fail(f"Authentication Failed. Check terminal for raw error dump.")
        
    access_token = result["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    print("✅ Authenticated! Testing SharePoint Site Access...")
    
    hostname = "dewyntersltd.sharepoint.com"
    site_path = "/sites/SALESREPORTING"
    graph_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
    
    print(f"📡 Requesting: {graph_url}")
    response = requests.get(graph_url, headers=headers)
    
    # Expose the EXACT Microsoft Graph API payload if it fails
    if response.status_code != 200:
        print(f"\n❌ HTTP {response.status_code} REJECTED!")
        print(f"📄 RAW MICROSOFT GRAPH ERROR:\n{response.text}\n")
        pytest.fail(f"Graph API rejected the request. Status {response.status_code}. See terminal for raw JSON.")
        
    # Success path
    site_data = response.json()
    print("\n🎉 SUCCESS! Connected to SharePoint.")
    print(f"📌 Site Name: {site_data.get('displayName')}")
    print(f"🔗 Web URL:   {site_data.get('webUrl')}")
    print(f"🔑 Site ID:   {site_data.get('id')}  <-- (SAVE THIS FOR LATER!)")
    
    assert "id" in site_data
