import os
import json
import shutil
import base64
import importlib
import pandas as pd
import requests
import msal
import time
from datetime import timezone, timedelta
from dateutil import parser as date_parser
from prefect import flow, task, get_run_logger

# --- Local Imports ---
import outlook_utils as utils

# --- Configuration Load ---
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
    # Parse ISO date and force conversion to UTC/GMT
    dt = date_parser.parse(received_date_str).astimezone(timezone.utc)
    # Subtract 1 day to reflect the actual reporting period
    report_date = dt - timedelta(days=1)
    date_formatted = report_date.strftime("%d_%m_%y")
    
    filename = (
        f"{metadata['show_name']}_"
        f"{metadata['venue_name']}_"
        f"{metadata['show_id']}_"
        f"{metadata['venue_id']}_"
        f"{metadata['document_id']}_"
        f"{date_formatted}"
        f"{extension}"
    )
    return filename.replace(" ", "_").replace("/", "-")

# --- Tasks ---

@task(name="fetch_historical_backlog", retries=2)
def fetch_and_route_emails():
    """
    Uses Microsoft Graph $search to perform targeted historical lookups based on JSON config rules,
    bypassing unrelated emails and safely paging through deep backlogs.
    """
    logger = get_run_logger()
    token = get_graph_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'ConsistencyLevel': 'eventual' 
    }
    
    processed_ids = load_processed_ids()
    candidates = []

    logger.info(f"🔎 Initiating backlog search across {len(CONFIG['rules'])} active rules.")

    for rule in CONFIG['rules']:
        if not rule.get('active'): continue

        crit = rule['match_criteria']
        sender = crit['sender_domain']
        subject_kw = crit['subject_keyword']
        search_query = f'from:"{sender}" AND subject:"{subject_kw}"'

        endpoint = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages"
        params = {
            '$search': f'"{search_query}"',
            '$select': 'id,subject,from,hasAttachments,receivedDateTime',
            '$top': 100 
        }

        logger.info(f"--- 📡 Searching for Rule: {rule['rule_name']} ---")
        logger.info(f"   Query: {search_query}")
        
        page_count = 1
        total_found_for_rule = 0
        total_skipped_for_rule = 0

        while endpoint:
            try:
                if page_count > 1:
                    logger.info(f"   📄 Fetching page {page_count}...")
                    
                response = requests.get(endpoint, headers=headers, params=params)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    logger.warning(f"   ⚠️ Rate limited (429)! Sleeping for {retry_after}s.")
                    time.sleep(retry_after)
                    continue 
                
                response.raise_for_status()
                data = response.json()
                emails = data.get('value', [])
                
                if not emails and page_count == 1:
                    logger.info(f"   🤷‍♂️ No emails found matching criteria.")
                    break

                for email in emails:
                    msg_id = email['id']
                    
                    if msg_id in processed_ids:
                        total_skipped_for_rule += 1
                        continue
                        
                    if not email.get('hasAttachments'):
                        continue

                    total_found_for_rule += 1
                    candidates.append({
                        "id": msg_id,
                        "subject": email.get('subject'),
                        "received_date": email.get('receivedDateTime'),
                        "rule": rule
                    })

                endpoint = data.get('@odata.nextLink')
                params = None 
                
                if endpoint:
                    page_count += 1
                    time.sleep(0.5) 

            except Exception as e:
                logger.error(f"   ❌ Graph API Error: {e}")
                break 

        if total_found_for_rule > 0 or total_skipped_for_rule > 0:
            logger.info(f"   🏁 Rule Summary: Queued {total_found_for_rule} new emails. Skipped {total_skipped_for_rule} already processed.")

    logger.info(f"✅ Full search complete. Total new emails to process: {len(candidates)}")        
    return candidates

@task(name="process_email_attachment")
def process_email_attachment(email_meta):
    logger = get_run_logger()
    msg_id = email_meta['id']
    rule = email_meta['rule']
    meta = rule['metadata']
    proc_config = rule['processing']
    expected_ext = rule['match_criteria'].get('attachment_type', '').lower()
    
    logger.info(f"🚀 Processing Rule: {rule['rule_name']} | Subject: {email_meta['subject']}")
    
    token = get_graph_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    att_url = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages/{msg_id}/attachments"
    resp = requests.get(att_url, headers=headers)
    if resp.status_code != 200: return

    attachments = resp.json().get('value', [])
    if not attachments:
        save_processed_id(msg_id)
        return

    target_att = None
    actual_exts = []
    
    for att in attachments:
        ext = os.path.splitext(att['name'])[1].lower()
        actual_exts.append(ext)
        if ext == expected_ext:
            target_att = att
            break

    if not target_att:
        error_msg = f"The attachment type expected was {expected_ext}, but received {', '.join(actual_exts)}"
        logger.warning(f"[{rule['rule_name']}] {error_msg}")
        utils.send_teams_notification(f"⚠️ **Attachment Mismatch**\nRule: {rule['rule_name']}\n{error_msg}", logger)
        save_processed_id(msg_id)
        return

    std_filename_full = generate_standard_filename(meta, email_meta['received_date'], expected_ext)
    inbox_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['inbox'])
    temp_path = os.path.join(inbox_dir, std_filename_full)
    
    try:
        with open(temp_path, 'wb') as f:
            f.write(base64.b64decode(target_att['contentBytes']))
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        return

    try:
        parser_module = importlib.import_module(proc_config['parser_module'])
        parser_function = getattr(parser_module, proc_config['parser_function'])
        
        parsed_data, parse_logs = parser_function(temp_path)
        
        for line in parse_logs:
            if "CRITICAL" in line or "MISMATCH" in line:
                logger.warning(f"[{rule['rule_name']}] {line}")

        if not parsed_data:
            raise ValueError("Parser returned 0 rows.")

        df = pd.DataFrame(parsed_data)
        
        if proc_config.get('needs_lookup'):
            lookup_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['lookups'])
            lookup_file = os.path.join(lookup_dir, f"{meta['show_id']}_{meta['venue_id']}_event_dates.csv")
            
            if os.path.exists(lookup_file):
                lookup_df = pd.read_csv(lookup_file)
                df['Performance/Event Code'] = df['Performance/Event Code'].astype(str).str.strip()
                lookup_df['Show Code'] = lookup_df['Show Code'].astype(str).str.strip()
                
                df = df.merge(
                    lookup_df[['Show Code', 'Performance Date Time']],
                    left_on='Performance/Event Code',
                    right_on='Show Code',
                    how='left'
                )
            else:
                logger.warning(f"   ⚠️ Lookup file needed but missing: {lookup_file}")

        csv_name = std_filename_full.replace(expected_ext, ".csv")
        csv_path = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['processed'], csv_name)
        df.to_csv(csv_path, index=False)
        
        archive_path = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['archive'], std_filename_full)
        shutil.move(temp_path, archive_path)
        
        logger.info(f"✅ Saved CSV: {csv_name}")
        utils.send_teams_notification(f"✅ **Extraction Success**\n\nRule: {rule['rule_name']}\nRows: {len(df)}\nFile: {csv_name}", logger)

    except Exception as e:
        logger.error(f"❌ Processing Error: {e}")
        utils.send_teams_notification(f"❌ Failed: {rule['rule_name']}\n{e}", logger)
        
        if os.path.exists(temp_path):
            failed_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['failed'])
            shutil.move(temp_path, os.path.join(failed_dir, std_filename_full))

    save_processed_id(msg_id)

@flow(name="Email Extraction Flow", log_prints=True)
def email_extraction_flow():
    candidates = fetch_and_route_emails()
    for email in candidates:
        process_email_attachment(email)

if __name__ == "__main__":
    email_extraction_flow.serve(name="outlook-extraction-service", cron="*/15 * * * *")