import os
import json
import shutil
import base64
import gc
import importlib
import pandas as pd
import requests
import msal
from datetime import datetime
from prefect import flow, task, get_run_logger
from dateutil import parser as date_parser

# --- Local Imports ---
import outlook_utils as utils

# --- Configuration Load ---
# Helper to load config relative to this script
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, "config", "show_reporting_rules.json")

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)

CONFIG = load_config()
GLOBAL = CONFIG['global_settings']

# Azure Credentials
utils.setup_environment()
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TARGET_EMAIL_USER = os.getenv("TARGET_EMAIL_USER")

# --- Directory Setup ---
# Ensure all directories from JSON exist
for key, relative_path in GLOBAL['data_dirs'].items():
    full_path = os.path.join(GLOBAL['base_dir'], relative_path)
    os.makedirs(full_path, exist_ok=True)

HISTORY_FILE = os.path.join(GLOBAL['base_dir'], GLOBAL['history_file'])
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

def generate_standard_filename(metadata, received_date_str, extension):
    """
    Format: {show}_{venue}_{showid}_{venueid}_{documentid}_{dd_mm_yy}.{ext}
    Crucial: Date is EMAIL RECEIVED DATE, not today.
    """
    # Parse ISO date from Graph API (e.g., 2023-10-15T14:30:00Z)
    dt = date_parser.parse(received_date_str)
    date_formatted = dt.strftime("%d_%m_%y")
    
    filename = (
        f"{metadata['show_name']}_"
        f"{metadata['venue_name']}_"
        f"{metadata['show_id']}_"
        f"{metadata['venue_id']}_"
        f"{metadata['document_id']}_"
        f"{date_formatted}"
        f"{extension}"
    )
    # Sanitize filename
    return filename.replace(" ", "_").replace("/", "-")

# --- Tasks ---

@task(name="fetch_and_route_emails", retries=2)
def fetch_and_route_emails():
    logger = get_run_logger()
    token = get_graph_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # Fetch top 50 newest messages
    endpoint = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages"
    params = {
        '$top': 50,
        '$select': 'id,subject,from,hasAttachments,receivedDateTime',
        '$orderby': 'receivedDateTime DESC'
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

    logger.info(f"🔎 Scanning {len(emails)} emails against {len(CONFIG['rules'])} active rules.")

    for email in emails:
        msg_id = email['id']
        
        if msg_id in processed_ids or not email.get('hasAttachments'):
            continue

        subject = (email.get('subject') or "").lower()
        sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
        received_date = email.get('receivedDateTime')

        # Match against JSON rules
        matched_rule = None
        for rule in CONFIG['rules']:
            if not rule.get('active'): continue
            
            crit = rule['match_criteria']
            # Check Sender Domain (flexible containment check)
            if crit['sender_domain'].lower() in sender:
                # Check Subject Keyword
                if crit['subject_keyword'].lower() in subject:
                    matched_rule = rule
                    break
        
        if matched_rule:
            candidates.append({
                "id": msg_id,
                "subject": email.get('subject'),
                "received_date": received_date,
                "rule": matched_rule
            })
            
    return candidates

@task(name="process_email_attachment")
def process_email_attachment(email_meta):
    logger = get_run_logger()
    msg_id = email_meta['id']
    rule = email_meta['rule']
    meta = rule['metadata']
    proc_config = rule['processing']
    
    logger.info(f"🚀 Processing Rule: {rule['rule_name']} | Subject: {email_meta['subject']}")
    
    token = get_graph_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # 1. Get Attachments
    att_url = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages/{msg_id}/attachments"
    resp = requests.get(att_url, headers=headers)
    if resp.status_code != 200: return

    attachments = resp.json().get('value', [])
    if not attachments:
        save_processed_id(msg_id)
        return

    # Process first attachment
    att = attachments[0]
    original_filename = att['name']
    file_ext = os.path.splitext(original_filename)[1].lower()
    
    # 2. Download to Inbox (Temp)
    inbox_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['inbox'])
    temp_path = os.path.join(inbox_dir, original_filename)
    
    try:
        with open(temp_path, 'wb') as f:
            f.write(base64.b64decode(att['contentBytes']))
        gc.collect() 
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        return

    # 3. Dynamic Parsing
    parse_success = False
    try:
        # Import module dynamically from JSON config string
        module_name = proc_config['parser_module']
        func_name = proc_config['parser_function']
        
        parser_module = importlib.import_module(module_name)
        parser_function = getattr(parser_module, func_name)
        
        # Execute Parser
        parsed_data, parse_logs = parser_function(temp_path)
        
        # Log Parser Feedback
        for line in parse_logs:
            if "CRITICAL" in line or "MISMATCH" in line:
                logger.warning(f"[{rule['rule_name']}] {line}")

        if parsed_data:
            df = pd.DataFrame(parsed_data)
            
            # 4. Lookup Enrichment (Optional)
            if proc_config.get('needs_lookup'):
                lookup_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['lookups'])
                lookup_file = os.path.join(lookup_dir, f"{meta['show_id']}_event_dates.csv")
                
                if os.path.exists(lookup_file):
                    logger.info(f"   Using Lookup: {lookup_file}")
                    lookup_df = pd.read_csv(lookup_file)
                    # Attempt merge on 'Date' or similar - simplistic join for now
                    # In production, ensure join keys match parser output
                    # df = df.merge(lookup_df, on='Date', how='left') 
                else:
                    logger.warning(f"   ⚠️ Lookup file needed but missing: {lookup_file}")

            # 5. Generate Standard Filenames
            std_filename_base = generate_standard_filename(meta, email_meta['received_date'], "")
            
            # Save CSV
            csv_name = f"{std_filename_base}.csv"
            csv_path = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['processed'], csv_name)
            df.to_csv(csv_path, index=False)
            
            # 6. Archive Original File (Renamed)
            archive_name = f"{std_filename_base}{file_ext}"
            archive_path = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['archive'], archive_name)
            
            shutil.move(temp_path, archive_path)
            
            logger.info(f"✅ Saved CSV: {csv_name}")
            logger.info(f"✅ Archived Raw: {archive_name}")
            
            utils.send_teams_notification(
                f"✅ **Extraction Success**\n\nRule: {rule['rule_name']}\nRows: {len(df)}\nFile: {csv_name}", logger
            )
            parse_success = True
        else:
            logger.warning("⚠️ Parser returned 0 rows.")

    except Exception as e:
        logger.error(f"❌ Processing Error: {e}")
        utils.send_teams_notification(f"❌ Failed: {rule['rule_name']}\n{e}", logger)
    
    # 7. Cleanup Failed Files
    if not parse_success and os.path.exists(temp_path):
        failed_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['failed'])
        try:
            shutil.move(temp_path, os.path.join(failed_dir, original_filename))
        except: pass

    save_processed_id(msg_id)
    gc.collect()

@flow(name="Email Extraction Flow", log_prints=True)
def email_extraction_flow():
    candidates = fetch_and_route_emails()
    for email in candidates:
        process_email_attachment(email)

if __name__ == "__main__":
    email_extraction_flow.serve(name="outlook-extraction-service", cron="* * * * *")