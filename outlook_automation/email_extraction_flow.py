import sys
from pathlib import Path
import os
import json
import shutil
import base64
import importlib
import pandas as pd
import requests
import msal
import time
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

# --- Local Imports ---
from outlook_automation import utils

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
TARGET_EMAIL_USER = os.getenv("FIGURES_INBOX_ADDRESS")

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
    """Loads processed IDs, supporting both new CSV audit format and legacy single-ID lines."""
    processed = set()
    with open(HISTORY_FILE, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 2:
                processed.add(parts[1]) # Index 1 is the msg_id in the new CSV format
            elif len(parts) == 1 and parts[0]:
                processed.add(parts[0]) # Legacy fallback
    return processed

def save_processed_id(msg_id, rule_name):
    """Saves to audit log in CSV format: timestamp,msg_id,rule_name"""
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(HISTORY_FILE, 'a') as f:
        f.write(f"{timestamp},{msg_id},{rule_name}\n")

def generate_standard_filename(metadata, received_date_str, extension):
    # Parse ISO date and force conversion to UTC/GMT
    dt = date_parser.parse(received_date_str).astimezone(timezone.utc)
    # Subtract 1 day to reflect the actual reporting period (T-1)
    report_date = dt - timedelta(days=1)
    date_formatted = report_date.strftime("%d_%m_%Y")
    
    filename = (
        f"{metadata['show_name']}."
        f"{metadata['venue_name']}."
        f"{metadata['show_id']}_"
        f"{metadata['venue_id']}_"
        f"{metadata['document_id']}_"
        f"{date_formatted}"
        f"{extension}"
    )
    return filename.replace(" ", "-").replace("/", "-")

# --- Tasks ---
@task(name="fetch_historical_backlog", retries=2)
def fetch_and_route_emails():
    logger = get_run_logger()
    token = get_graph_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    processed_ids = load_processed_ids()
    candidates = []

    logger.info(f"🔎 Initiating search across {len(CONFIG['rules'])} active rules.")

    for rule in CONFIG['rules']:
        if not rule.get('active'): continue

        crit = rule['match_criteria']
        sender = crit['sender_domain'].lower()
        subject_kw = crit['subject_keyword'].lower()
        
        # Parse backfill_since to a UTC datetime object for strict Python comparison
        backfill_str = rule.get('backfill_since', '2000-01-01')
        backfill_dt = date_parser.parse(backfill_str).replace(tzinfo=timezone.utc)

        # 1. Fuzzy Text Search (Drastically reduces payload to ONLY relevant emails)
        search_query = f'"{sender} {subject_kw}"'

        endpoint = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages"
        params = {
            '$search': search_query,
            '$select': 'id,subject,from,hasAttachments,receivedDateTime',
            '$top': 100
        }

        logger.info(f"--- 📡 Searching for Rule: {rule['rule_name']} ---")
        logger.info(f"   Date Boundary: >= {backfill_str}")
        
        page_count = 1
        total_found = 0
        total_skipped = 0
        total_out_of_bounds = 0

        while endpoint:
            try:
                if page_count > 1: logger.info(f"   📄 Fetching page {page_count}...")
                    
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
                    logger.info(f"   🤷‍♂️ No emails found matching text criteria.")
                    break

                for email in emails:
                    msg_id = email['id']
                    
                    # 2. Strict Python Validation
                    actual_subject = (email.get('subject') or "").lower()
                    actual_sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
                    
                    if sender not in actual_sender or subject_kw not in actual_subject:
                        continue 

                    # 3. Strict Python Date Boundary check
                    email_date_str = email.get('receivedDateTime')
                    email_dt = date_parser.parse(email_date_str).astimezone(timezone.utc)
                    
                    if email_dt < backfill_dt:
                        total_out_of_bounds += 1
                        continue # Skip emails older than the backfill_since date
                        
                    if msg_id in processed_ids:
                        total_skipped += 1
                        continue
                        
                    if not email.get('hasAttachments'):
                        continue

                    total_found += 1
                    candidates.append({
                        "id": msg_id,
                        "subject": email.get('subject'),
                        "received_date": email_date_str,
                        "rule": rule
                    })

                endpoint = data.get('@odata.nextLink')
                params = None # Clear params because nextLink handles them automatically
                
                if endpoint:
                    page_count += 1
                    time.sleep(0.5) 

            except Exception as e:
                logger.error(f"   ❌ Graph API Error: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"   API Details: {e.response.text}")
                break 

        if total_found > 0 or total_skipped > 0 or total_out_of_bounds > 0:
            logger.info(f"   🏁 Rule Summary: Queued {total_found} new. Skipped {total_skipped} processed. Ignored {total_out_of_bounds} older than backfill date.")

    return candidates

@task(name="process_email_attachment")
def process_email_attachment(email_meta):
    """Returns a tuple: (Success_Boolean, Received_Date_Str, Rule_Name)"""
    logger = get_run_logger()
    msg_id = email_meta['id']
    rule = email_meta['rule']
    meta = rule['metadata']
    proc_config = rule['processing']
    r_name = rule['rule_name']
    expected_ext = rule['match_criteria'].get('attachment_type', '').lower()
    
    logger.info(f"🚀 Processing Rule: {r_name} | Subject: {email_meta['subject']}")
    
    token = get_graph_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    att_url = f"https://graph.microsoft.com/v1.0/users/{TARGET_EMAIL_USER}/messages/{msg_id}/attachments"
    resp = requests.get(att_url, headers=headers)
    if resp.status_code != 200: 
        return False, None, r_name

    attachments = resp.json().get('value', [])
    if not attachments:
        save_processed_id(msg_id, r_name)
        return False, None, r_name

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
        logger.warning(f"[{r_name}] {error_msg}")
        utils.send_teams_notification(f"⚠️ **Attachment Mismatch**\n\n**Rule:** {r_name}\n**Error:** {error_msg}", logger)
        save_processed_id(msg_id, r_name)
        return False, None, r_name

    std_filename_full = generate_standard_filename(meta, email_meta['received_date'], expected_ext)
    inbox_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['inbox'])
    temp_path = os.path.join(inbox_dir, std_filename_full)
    
    try:
        with open(temp_path, 'wb') as f:
            f.write(base64.b64decode(target_att['contentBytes']))
        logger.info(f"   📥 Downloaded matching attachment to inbox directory.")
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        return False, None, r_name

    try:
        logger.info(f"   ⚙️ Initializing parser: {proc_config['parser_module']}...")
        parser_module = importlib.import_module(proc_config['parser_module'])
        parser_function = getattr(parser_module, proc_config['parser_function'])
        
        # UNPACKING NEW DATA CONTRACT
        parsed_data, validation_result = parser_function(temp_path)

        if not parsed_data:
            raise ValueError("Parser completed but returned 0 rows.")

        # Hard Fail evaluation
        if validation_result.status == "FAILED":
            raise ValueError(f"Data Validation Failed: {validation_result.message}")

        # Prefect Artifact Generation
        md_table = (
            f"## Validation Result: {validation_result.status}\n\n"
            f"**Message:** {validation_result.message}\n\n"
            f"| Metric | Value |\n|---|---|\n"
        )
        for key, val in validation_result.metrics.items():
            md_table += f"| {key} | {val} |\n"
            
        create_markdown_artifact(
            key=f"val-{msg_id.lower()[:20]}", 
            markdown=md_table, 
            description=f"Validation details for {r_name}"
        )

        df = pd.DataFrame(parsed_data)
        matched_lookup_count = "N/A"
        
        if proc_config.get('needs_lookup'):
            lookup_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['lookups'])
            lookup_file = os.path.join(lookup_dir, f"{meta['show_id']}_{meta['venue_id']}_event_dates.csv")
            
            if os.path.exists(lookup_file):
                logger.info(f"   🔀 Attempting merge with lookup file: {os.path.basename(lookup_file)}")
                lookup_df = pd.read_csv(lookup_file)
                df['Performance/Event Code'] = df['Performance/Event Code'].astype(str).str.strip()
                lookup_df['Show Code'] = lookup_df['Show Code'].astype(str).str.strip()
                
                df = df.merge(
                    lookup_df[['Show Code', 'Performance Date Time']],
                    left_on='Performance/Event Code',
                    right_on='Show Code',
                    how='left'
                )
                
                matched_lookup_count = df['Performance Date Time'].notna().sum()
                logger.info(f"   ✅ Lookup merge complete. {matched_lookup_count}/{len(df)} rows matched.")
                
                if matched_lookup_count < len(df):
                    logger.warning(f"   ⚠️ Some Event Codes did not match the lookup file!")
            else:
                logger.warning(f"   ⚠️ Lookup file required but missing: {lookup_file}")

        csv_name = std_filename_full.replace(expected_ext, ".csv")
        csv_path = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['processed'], csv_name)
        df.to_csv(csv_path, index=False)
        
        archive_path = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['archive'], std_filename_full)
        shutil.move(temp_path, archive_path)
        
        logger.info(f"✅ Final CSV saved successfully: {csv_name}")
        
        # --- TEAMS NOTIFICATION LOGIC ---
        if validation_result.status == "UNVALIDATED":
            warning_msg = (
                f"⚠️ **Manual Review Required**\n\n"
                f"**Rule:** {r_name}\n"
                f"**File:** {csv_name}\n\n"
                f"**Message:** {validation_result.message}\n\n"
                f"📊 **Extraction Summary:**\n"
                f"- **Rows Extracted:** {len(df)}\n"
            )
            for k, v in validation_result.metrics.items():
                warning_msg += f"- **{k}:** {v}\n"
            warning_msg += f"- **Lookup Match:** {matched_lookup_count}/{len(df)} rows"
            
            utils.send_teams_notification(warning_msg, logger)
        else:
            # Silent Pass
            logger.info(f"✅ Teams Alert Bypassed: Validation PASSED natively.")

        save_processed_id(msg_id, r_name)
        return True, email_meta['received_date'], r_name

    except Exception as e:
        logger.error(f"❌ Processing Error: {e}")
        utils.send_teams_notification(f"❌ **Extraction Failed**\n\n**Rule:** {r_name}\n**Error:** {e}", logger)
        
        if os.path.exists(temp_path):
            failed_dir = os.path.join(GLOBAL['base_dir'], GLOBAL['data_dirs']['failed'])
            shutil.move(temp_path, os.path.join(failed_dir, std_filename_full))
            
        save_processed_id(msg_id, r_name)
        return False, None, r_name

@task(name="update_config_state")
def update_config_state(successful_runs):
    """Updates the JSON config with the most recent successfully processed date per rule."""
    if not successful_runs: return
    logger = get_run_logger()
    
    # 1. Find the latest (max) date for each successfully processed rule
    max_dates = {}
    for r_name, date_str in successful_runs:
        dt = date_parser.parse(date_str).astimezone(timezone.utc)
        if r_name not in max_dates or dt > max_dates[r_name]:
            max_dates[r_name] = dt
            
    # 2. Open, update, and save the JSON config file
    with open(CONFIG_PATH, 'r') as f:
        current_config = json.load(f)
        
    updated = False
    for rule in current_config['rules']:
        r_name = rule['rule_name']
        if r_name in max_dates:
            new_date_str = max_dates[r_name].strftime('%Y-%m-%d')
            current_date_str = rule.get('backfill_since', '1900-01-01')
            
            if new_date_str > current_date_str:
                rule['backfill_since'] = new_date_str
                updated = True
                logger.info(f"🔄 Advanced '{r_name}' backfill date to {new_date_str}")
                
    if updated:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(current_config, f, indent=4)
        logger.info("💾 Saved updated state to show_reporting_rules.json")

@flow(name="Email Extraction Flow", log_prints=True)
def email_extraction_flow():
    candidates = fetch_and_route_emails()
    successful_runs = []
    
    for email in candidates:
        success, rec_date, r_name = process_email_attachment(email)
        if success:
            successful_runs.append((r_name, rec_date))
            
    update_config_state(successful_runs)

if __name__ == "__main__":
    email_extraction_flow.serve(
        name="email-extraction-automated",
        cron="*/15 * * * *",  # Polling every 15m as per README
        tags=["medallion-raw", "production"],
        description="Automated extraction of email attachments to CSVs."
    )
