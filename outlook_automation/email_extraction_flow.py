import os
import gc
import json
import base64
import shutil
import requests
import msal
import pandas as pd
from datetime import datetime
from prefect import flow, task, get_run_logger

# --- LOCAL IMPORTS ---
# We assume brandwatch_utils is copied to this folder or in python path
import outlook_utils as utils
from parsers import malvern_theatre_parser, sistic_agency_parser

# --- CONFIGURATION ---
utils.setup_environment()

# Azure / Graph Settings
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TARGET_MAILBOX = os.getenv("TARGET_MAILBOX")  # e.g., "shared.mailbox@dewynters.com"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]

# Paths
BASE_DIR = r"C:\Prefect\outlook_automation"
DATA_DIRS = {
    "inbox": os.path.join(BASE_DIR, "data", "inbox"),
    "processed": os.path.join(BASE_DIR, "data", "processed"),
    "archive": os.path.join(BASE_DIR, "data", "archive"),
    "failed": os.path.join(BASE_DIR, "data", "failed"),
}

# Ensure directories exist
for d in DATA_DIRS.values():
    os.makedirs(d, exist_ok=True)

# --- HELPER TASKS ---

@task(name="authenticate_graph", retries=3)
def authenticate_graph():
    """Acquires a token from MSAL for Microsoft Graph."""
    logger = get_run_logger()
    
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    
    result = app.acquire_token_for_client(scopes=SCOPE)
    
    if "access_token" in result:
        logger.info("✅ Azure Auth Successful")
        return result["access_token"]
    else:
        logger.error(f"❌ Azure Auth Failed: {result.get('error')}")
        raise Exception("Could not authenticate with Microsoft Graph")

@task(name="fetch_messages")
def fetch_messages(access_token):
    """
    Fetches metadata for the top 10 unread emails with attachments.
    We do NOT download attachments here to save RAM.
    """
    logger = get_run_logger()
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Filter: Unread, Has Attachments
    # Select: Only ID, Subject, From, ReceivedDateTime (keep payload small)
    endpoint = (
        f"https://graph.microsoft.com/v1.0/users/{TARGET_MAILBOX}/messages"
        "?$filter=isRead eq false and hasAttachments eq true"
        "&$select=id,subject,from,receivedDateTime,hasAttachments"
        "&$top=10"
    )

    response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        messages = response.json().get("value", [])
        logger.info(f"📬 Found {len(messages)} unread messages with attachments.")
        return messages
    else:
        logger.error(f"❌ Failed to fetch messages: {response.text}")
        return []

@task(name="process_single_message")
def process_single_message(message, access_token):
    """
    Downloads attachment -> Routes to Parser -> Saves CSV -> Cleans RAM.
    """
    logger = get_run_logger()
    headers = {"Authorization": f"Bearer {access_token}"}
    
    msg_id = message["id"]
    subject = message["subject"]
    sender = message["from"]["emailAddress"]["address"].lower()
    
    logger.info(f"📧 Processing: '{subject}' from {sender}")

    # 1. ROUTING LOGIC
    parser_module = None
    parser_name = ""
    
    if "figures@mallvern-theatres.com" in sender and "The Silence of the Lambs" in subject:
        parser_module = malvern_theatre_parser
        parser_name = "Malvern"
    elif "sisticadmin@sistic.com.sg" in sender and "Jesus Christ Superstar" in subject:
        parser_module = sistic_agency_parser
        parser_name = "Sistic"
    else:
        logger.warning(f"⏩ Skipping: No matching route for '{subject}'")
        return "skipped"

    try:
        # 2. FETCH ATTACHMENTS (Only for the matched message)
        att_url = f"https://graph.microsoft.com/v1.0/users/{TARGET_MAILBOX}/messages/{msg_id}/attachments"
        att_resp = requests.get(att_url, headers=headers)
        
        if att_resp.status_code != 200:
            raise Exception(f"Failed to get attachments: {att_resp.text}")
            
        attachments = att_resp.json().get("value", [])
        
        for att in attachments:
            filename = att["name"]
            
            # Filter specific file types based on the parser
            if parser_name == "Malvern" and not filename.lower().endswith(".pdf"):
                continue
            if parser_name == "Sistic" and not filename.lower().endswith((".xls", ".xlsx")):
                continue

            logger.info(f"⬇️ Downloading attachment: {filename}...")
            
            # 3. SAVE TO DISK (Critical for RAM)
            # Graph sends attachments as Base64 strings. We decode and write directly to disk.
            file_content = base64.b64decode(att["contentBytes"])
            input_path = os.path.join(DATA_DIRS["inbox"], filename)
            
            with open(input_path, "wb") as f:
                f.write(file_content)
            
            # Explicitly delete the content variable to free memory immediately
            del file_content
            
            # 4. EXECUTE PARSER
            logger.info(f"🤖 Running {parser_name} Parser...")
            
            # Note: The parsers return (data, logs). We only need data here.
            if parser_name == "Malvern":
                data, _ = parser_module.extract_contractual_report(input_path)
            else:
                data, _ = parser_module.extract_settlement_data(input_path)
            
            # 5. EXPORT CSV
            if data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_name = f"Clean_{parser_name}_{timestamp}.csv"
                csv_path = os.path.join(DATA_DIRS["processed"], csv_name)
                
                df = pd.DataFrame(data)
                df.to_csv(csv_path, index=False)
                logger.info(f"✅ Saved CSV: {csv_name} ({len(df)} rows)")
                
                # Cleanup: Move raw file to archive
                shutil.move(input_path, os.path.join(DATA_DIRS["archive"], f"{timestamp}_{filename}"))
                del df # Free DataFrame memory
            else:
                logger.warning(f"⚠️ No data extracted from {filename}")
                shutil.move(input_path, os.path.join(DATA_DIRS["failed"], filename))

    except Exception as e:
        logger.error(f"❌ Error processing {subject}: {str(e)}")
        # Move any stuck files to failed
        if 'input_path' in locals() and os.path.exists(input_path):
            shutil.move(input_path, os.path.join(DATA_DIRS["failed"], filename))
        return "failed"
    
    # 6. MARK AS READ (Optional - Handles Permission Error Gracefully)
    try:
        patch_payload = {"isRead": True}
        patch_resp = requests.patch(
            f"https://graph.microsoft.com/v1.0/users/{TARGET_MAILBOX}/messages/{msg_id}",
            headers=headers,
            json=patch_payload
        )
        if patch_resp.status_code == 200:
            logger.info("marked_read")
        else:
            logger.warning(f"⚠️ Could not mark as read (Permission Issue?): {patch_resp.status_code}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to update email status: {e}")

    return "success"

@flow(name="Email Extraction Pipeline", log_prints=True)
def email_extraction_flow():
    logger = get_run_logger()
    
    try:
        token = authenticate_graph()
        messages = fetch_messages(token)
        
        if not messages:
            logger.info("💤 No new matching emails found.")
            return

        success_count = 0
        
        for msg in messages:
            result = process_single_message(msg, token)
            if result == "success":
                success_count += 1
            
            # CRITICAL MEMORY MANAGEMENT
            # Force garbage collection after every single email
            gc.collect() 
            
        if success_count > 0:
            utils.send_teams_notification(
                f"✅ **Email Extraction Success**\n\nProcessed {success_count} reports successfully.", 
                logger
            )

    except Exception as e:
        logger.error(f"Critical Flow Failure: {e}")
        utils.send_teams_notification(
            f"❌ **Email Extraction Failed**\n\nError: {str(e)}", 
            logger
        )
        raise

if __name__ == "__main__":
    email_extraction_flow()