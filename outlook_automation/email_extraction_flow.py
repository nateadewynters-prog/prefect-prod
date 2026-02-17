import os
import shutil
import base64
import gc
import pandas as pd
import requests
import msal
from datetime import datetime
from prefect import flow, task, get_run_logger

# --- Local Imports ---
import outlook_utils as utils
from parsers import malvern_theatre_parser, sistic_agency_parser

# --- Configuration ---
utils.setup_environment()

# Azure / Graph API Credentials
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
# CRITICAL: Ensure this matches the inbox you sent the email to (e.g., figures@dewynters.com)
TARGET_EMAIL_USER = os.getenv("TARGET_EMAIL_USER")  

# Directory Setup
BASE_DIR = r"C:\Prefect\outlook_automation\data"
INBOX_DIR = os.path.join(BASE_DIR, "inbox")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
FAILED_DIR = os.path.join(BASE_DIR, "failed")
HISTORY_FILE = os.path.join(BASE_DIR, "processed_ids.txt")

# Ensure directories exist
for folder in [INBOX_DIR, PROCESSED_DIR, ARCHIVE_DIR, FAILED_DIR]:
    os.makedirs(folder, exist_ok=True)

if not os.path.exists(HISTORY_FILE):
    with open(HISTORY_FILE, 'w') as f: pass

# --- Helper Functions ---

def get_graph_token():
    authority_url = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority_url, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" in result:
        return result["access_token"]
    raise Exception(f"Failed to acquire Graph Token: {result.get('error_description')}")

def load_processed_ids():
    with open(HISTORY_FILE, 'r') as f:
        return set(line.strip() for line in f)

def save_processed_id(message_id):
    with open(HISTORY_FILE, 'a') as f:
        f.write(f"{message_id}\n")

# --- Tasks ---

@task(name="fetch_new_emails", retries=2)
def fetch_new_emails():
    logger = get_run_logger()
    token = get_graph_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # FIX: Fetch top 50 NEWEST messages (regardless of Read/Unread status)
    endpoint = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages"
    params = {
        '$top': 50,
        '$select': 'id,subject,from,hasAttachments,receivedDateTime',
        '$orderby': 'receivedDateTime DESC'  # <--- Critical: Newest first
    }
    
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        emails = response.json().get('value', [])
    except Exception as e:
        logger.error(f"Graph API Error: {e}")
        return []
    
    processed_ids = load_processed_ids()
    candidates = []

    logger.info(f"🔎 Scanned {len(emails)} recent emails (looking for 'Settlement' or 'Contractual').")

    for email in emails:
        msg_id = email['id']
        subject = email.get('subject', '') or ""
        sender_email = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
        
        # 1. Skip if already processed
        if msg_id in processed_ids:
            continue

        # 2. Skip if no attachments
        if not email.get('hasAttachments'):
            continue

        # 3. Routing Logic (Case Insensitive)
        route = None
        subj_lower = subject.lower()
        
        # Route A: Malvern
        if "malvern-theatres.com" in sender_email and "contractual report" in subj_lower:
            route = "MALVERN"
        
        # Route B: Sistic (AND generic testing)
        # Added 'dewynters.com' to sender check so you can test from your own email
        elif ("sistic.com.sg" in sender_email or "dewynters.com" in sender_email) and "settlement" in subj_lower:
            route = "SISTIC"
        
        if route:
            candidates.append({
                "id": msg_id,
                "subject": subject,
                "sender": sender_email,
                "route": route
            })
            
    if candidates:
        logger.info(f"✅ Found {len(candidates)} new actionable emails.")
    else:
        logger.info("ℹ️  No new matching emails found in the last 50.")
        
    return candidates

@task(name="process_email_attachment")
def process_email_attachment(email_meta):
    logger = get_run_logger()
    msg_id = email_meta['id']
    route = email_meta['route']
    subject = email_meta['subject']
    
    logger.info(f"🚀 Processing: '{subject}' (Route: {route})")
    
    token = get_graph_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # 1. Get Attachments
    att_url = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages/{msg_id}/attachments"
    resp = requests.get(att_url, headers=headers)
    
    if resp.status_code != 200:
        logger.error(f"Failed to fetch attachments: {resp.text}")
        return

    attachments = resp.json().get('value', [])
    if not attachments:
        save_processed_id(msg_id) 
        return

    # Process first attachment
    att = attachments[0]
    filename = att['name']
    
    # 2. Disk-First Save
    try:
        content_b64 = att['contentBytes']
        content_bytes = base64.b64decode(content_b64)
        del content_b64 
        
        temp_path = os.path.join(INBOX_DIR, filename)
        with open(temp_path, 'wb') as f:
            f.write(content_bytes)
        del content_bytes
        gc.collect() 

    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        return

    # 3. Parsing
    parse_success = False
    try:
        if route == "MALVERN":
            parsed_data, parse_logs = malvern_theatre_parser.extract_contractual_report(temp_path)
        elif route == "SISTIC":
            parsed_data, parse_logs = sistic_agency_parser.extract_settlement_data(temp_path)

        # Log errors from parser
        for line in parse_logs:
            if "CRITICAL" in line or "MISMATCH" in line:
                logger.warning(f"Parser: {line}")

        if parsed_data:
            parse_success = True
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_name = f"{route}_{timestamp}.csv"
            csv_path = os.path.join(PROCESSED_DIR, csv_name)
            
            pd.DataFrame(parsed_data).to_csv(csv_path, index=False)
            logger.info(f"✅ CSV Saved: {csv_name}")
            
            utils.send_teams_notification(
                f"✅ **Extraction Success: {route}**\n\nSubject: {subject}\nRows: {len(parsed_data)}", logger
            )
        else:
            logger.warning("⚠️ Parser returned 0 rows.")

    except Exception as e:
        logger.error(f"❌ Parsing Error: {e}")
        utils.send_teams_notification(f"❌ Failed: {route}\n{e}", logger)
    
    # 4. Cleanup & Mark Complete
    try:
        dest_dir = ARCHIVE_DIR if parse_success else FAILED_DIR
        shutil.move(temp_path, os.path.join(dest_dir, filename))
    except Exception: pass

    save_processed_id(msg_id)
    gc.collect()

@flow(name="Email Extraction Flow", log_prints=True)
def email_extraction_flow():
    candidates = fetch_new_emails()
    for email in candidates:
        process_email_attachment(email)

if __name__ == "__main__":
    email_extraction_flow.serve(name="outlook-extraction-service", cron="* * * * *")