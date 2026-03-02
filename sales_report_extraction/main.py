import os
import json
from datetime import timezone
from dateutil import parser as date_parser
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

# --- Absolute Imports from our new src/ layout ---
from src.env_setup import setup_environment
from src.notifications import send_teams_notification
from src.graph_client import GraphClient
from src.file_processor import ProcessingEngine
from src.sftp_client import upload_to_sftp

# 1. Setup Environment
setup_environment()

# 2. Load Configuration (Resolving path relative to this file)
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_ROOT, "config", "show_reporting_rules.json")

with open(CONFIG_PATH, 'r') as f:
    CONFIG = json.load(f)

# 3. Instantiate Domain Objects
graph = GraphClient(
    tenant_id=os.getenv("AZURE_TENANT_ID"),
    client_id=os.getenv("AZURE_CLIENT_ID"),
    client_secret=os.getenv("AZURE_CLIENT_SECRET"),
    target_user=os.getenv("FIGURES_INBOX_ADDRESS")
)
engine = ProcessingEngine(CONFIG['global_settings'], CONFIG_PATH)

@task(name="Fetch and Route Emails", retries=2)
def fetch_and_route_emails():
    logger = get_run_logger()
    processed_ids = engine.load_processed_ids()
    candidates = []

    logger.info(f"🔎 Initiating search across {len(CONFIG['rules'])} active rules.")

    for rule in CONFIG['rules']:
        if not rule.get('active'): continue

        crit = rule['match_criteria']
        search_query = f'"{crit["sender_domain"]} {crit["subject_keyword"]}"'
        backfill_dt = date_parser.parse(rule.get('backfill_since', '2000-01-01')).replace(tzinfo=timezone.utc)

        logger.info(f"--- 📡 Searching for Rule: {rule['rule_name']} ---")
        emails = graph.search_emails(search_query)

        for email in emails:
            email_dt = date_parser.parse(email['receivedDateTime']).astimezone(timezone.utc)
            
            # Strict boundary checks
            if email_dt < backfill_dt or email['id'] in processed_ids or not email.get('hasAttachments'):
                continue

            actual_sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
            if crit['sender_domain'].lower() in actual_sender:
                candidates.append({"email_data": email, "rule": rule})

    return candidates

@task(name="Process Email Attachment")
def process_email(candidate):
    logger = get_run_logger()
    email = candidate['email_data']
    rule = candidate['rule']
    r_name = rule['rule_name']
    msg_id = email['id']
    expected_ext = rule['match_criteria']['attachment_type'].lower()

    logger.info(f"🚀 Processing Rule: {r_name} | Subject: {email['subject']}")
    
    try:
        # 1. Download File
        content_bytes, _ = graph.download_attachment(msg_id, expected_ext)
        std_name = engine.generate_filename(rule['metadata'], email['receivedDateTime'], expected_ext)
        temp_path = os.path.join(engine.base_dir, engine.dirs['inbox'], std_name)
        
        with open(temp_path, 'wb') as f: 
            f.write(content_bytes)

        # 2. Process File
        df, validation_result, csv_path = engine.process_file(temp_path, rule)

        # 2.5 Upload to SFTP if processing was successful
        upload_to_sftp(local_file_path=csv_path, filename=os.path.basename(csv_path))

        # 3. Create Artifacts & Alerts
        md_table = f"## Validation Result: {validation_result.status}\n\n**Message:** {validation_result.message}\n\n| Metric | Value |\n|---|---|\n"
        for k, v in validation_result.metrics.items(): 
            md_table += f"| {k} | {v} |\n"
            
        create_markdown_artifact(key=f"val-{msg_id[:15].lower()}", markdown=md_table, description=r_name)

        if validation_result.status == "UNVALIDATED":
            send_teams_notification(f"⚠️ **Manual Review Required**\n\n**Rule:** {r_name}\n**Message:** {validation_result.message}", logger)

        engine.save_processed_id(msg_id, r_name)
        return True, email['receivedDateTime'], r_name

    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        engine.handle_failure(temp_path if 'temp_path' in locals() else "")
        send_teams_notification(f"❌ **Extraction Failed**\n\n**Rule:** {r_name}\n**Error:** {str(e)}", logger)
        engine.save_processed_id(msg_id, r_name)
        return False, None, r_name

@task(name="Update State")
def update_state(successful_runs):
    logger = get_run_logger()
    if successful_runs:
        engine.update_config_state(successful_runs)
        logger.info("💾 Saved updated state to show_reporting_rules.json")

@flow(name="Sales Extractor Flow", log_prints=True)
def sales_extractor_flow():
    candidates = fetch_and_route_emails()
    successful_runs = []
    
    for candidate in candidates:
        success, rec_date, r_name = process_email(candidate)
        if success:
            successful_runs.append((r_name, rec_date))
            
    update_state(successful_runs)

if __name__ == "__main__":
    sales_extractor_flow.serve(
        name="sales-extractor-flow",
        cron="*/15 * * * *",
        tags=["medallion-raw", "production"],
        description="Automated extraction of email attachments to CSVs."
    )
