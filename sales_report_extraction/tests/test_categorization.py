import os
import requests # Added for verification step
from src.env_setup import setup_environment
from src.graph_client import GraphClient
from prefect import flow  # <--- Add this import

@flow(name="Test Graph Client") # <--- Add this decorator
def run_test():
    # ... rest of your code remains the same ...
    # 1. Load environment variables
    setup_environment()
    
    # 2. Instantiate the Graph Client
    graph = GraphClient(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        target_user=os.getenv("FIGURES_INBOX_ADDRESS")
    )
    
    # 3. Define the Malvern search query
    search_query = '"figures@malvern-theatres.co.uk Malvern Theatres Figures The Silence of the Lambs"'
    tag_to_apply = "sales_report_extracted"
    
    print(f"🔎 Searching for latest Malvern emails...")
    
    # Fetch emails
    emails = graph.search_emails(search_query, top=2)
    
    if not emails:
        print("📭 No emails found to tag.")
        return

    print(f"✅ Found {len(emails)} emails. Attempting to tag them...")

    # 4. Loop, tag, and verify
    for i, email in enumerate(emails):
        msg_id = email['id']
        subject = email.get('subject', 'No Subject')
        date_received = email.get('receivedDateTime', 'Unknown Date')
        sender = email.get('from', {}).get('emailAddress', {}).get('address', 'Unknown Sender')
        
        print(f"\n--- Email {i+1} ---")
        print(f"📅 Date:    {date_received}")
        print(f"👤 Sender:  {sender}")
        print(f"📝 Subject: {subject}")
        
        try:
            # Attempt to apply the tag
            graph.tag_email(msg_id, tag_to_apply)
            
            # --- VERIFICATION STEP ---
            # Fetch the email back from Microsoft to see what categories it actually holds now
            verify_url = f"{graph.base_url}/users/{graph.target_user}/messages/{msg_id}?$select=categories"
            verify_resp = requests.get(verify_url, headers=graph.get_headers())
            
            if verify_resp.status_code == 200:
                saved_categories = verify_resp.json().get('categories', [])
                print(f"🔍 Backend Categories Check: {saved_categories}")
                
                if tag_to_apply in saved_categories:
                    print(f"✅ SUCCESS: The API confirms '{tag_to_apply}' is firmly attached to this email!")
                else:
                    print(f"⚠️ WARNING: API returned 200 OK, but the category didn't stick. We might need a different payload structure.")
            else:
                print(f"⚠️ Could not verify backend state. Status: {verify_resp.status_code}")
                
        except Exception as e:
            print(f"❌ Failed to tag: {e}")

if __name__ == "__main__":
    run_test()