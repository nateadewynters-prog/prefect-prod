import os
import requests
import msal
from src.env_setup import setup_environment

def get_sharepoint_tree():
    """Prints a visual tree of the SharePoint Documents library."""
    setup_environment()
    
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    site_id = os.getenv("SHAREPOINT_SALES_REPORTING_SITE_ID")
    
    if not all([tenant_id, client_id, client_secret, site_id]):
        print("❌ Missing credentials or SHAREPOINT_SITE_ID in .env!")
        return
        
    print("🔐 Authenticating...")
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret
    )
    
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        print(f"❌ Auth Failed: {result.get('error_description')}")
        return
        
    headers = {
        "Authorization": f"Bearer {result['access_token']}",
        "Accept": "application/json"
    }
    
    print(f"✅ Authenticated! Scanning SharePoint Drive...\n")
    print("📁 Root (Documents)")
    
    # Recursive function to map the folders
    def fetch_children(item_id, indent="", current_depth=1, max_depth=3):
        if current_depth > max_depth:
            return
            
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/children?$select=id,name,folder"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"{indent}❌ Error reading folder: {response.text}")
            return
            
        items = response.json().get('value', [])
        
        # Filter to ONLY show folders to keep the tree clean
        folders = [item for item in items if 'folder' in item]
        
        for i, folder in enumerate(folders):
            is_last = (i == len(folders) - 1)
            branch = "└── " if is_last else "├── "
            print(f"{indent}{branch}📁 {folder['name']}")
            
            # Prepare the indent for the next level down
            next_indent = indent + ("    " if is_last else "│   ")
            fetch_children(folder['id'], next_indent, current_depth + 1, max_depth)

    # Start the recursive scan from the root of the drive
    fetch_children("root")
    print("\n✅ Scan complete.")

if __name__ == "__main__":
    get_sharepoint_tree()
