import os
import json
from datetime import datetime, timezone, timedelta
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
def fetch_and_route_emails(days_back: int, target_rule: str | None = None):
    logger = get_run_logger()
    queued_sales_reports = []

    # 🚀 Calculate the dynamic start date based on the parameter
    start_date_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    start_date_str = start_date_dt.strftime('%Y-%m-%d')
    
    logger.info(f"🔎 Scanning for untagged emails received since {start_date_str}")

    for rule in CONFIG['rules']:
        if not rule.get('active'): continue
        
        # 🚀 BACKFILL LOGIC: If a specific target rule is provided, skip all others
        if target_rule and rule['rule_name'] != target_rule:
            continue

        crit = rule['match_criteria']
        
        # Append KQL received date filter to the Graph API search query
        search_query = f'"{crit["subject_keyword"]}"'

        logger.info(f"--- 📡 Searching for Rule: {rule['rule_name']} ---")
        emails = graph.search_emails(search_query)
        
        skipped = 0

        for email in emails:
            email_dt = date_parser.parse(email['receivedDateTime']).astimezone(timezone.utc)

            # Define existing_tags before using it
            existing_tags = email.get('categories', [])
            
            if email_dt < start_date_dt or "sales_report_extracted" in existing_tags or "sales_report_failed" in existing_tags or not email.get('hasAttachments'):
                skipped += 1
                continue

            # This line must align with the 'if' block above
            actual_sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
            
            if crit['sender_domain'].lower() in actual_sender:
                queued_sales_reports.append({"email_data": email, "rule": rule})
            else:
                skipped += 1
                
        logger.info(f"📊 Rule '{rule['rule_name']}': Found {len(emails)} total, Skipped {skipped}, Queued Sales Reports {len(emails) - skipped}")

    return queued_sales_reports

@task(name="Process Email Attachment")
def process_email(queued_sales_report):
    logger = get_run_logger()
    email = queued_sales_report['email_data']
    rule = queued_sales_report['rule']
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
            f.flush()               # <-- Force write out of Python buffer
            os.fsync(f.fileno())    # <-- Force write out of OS buffer

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

        # Tag as processed so it doesn't get picked up again
        graph.tag_email(msg_id, "sales_report_extracted")
        return True, email['receivedDateTime'], r_name

    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        engine.handle_failure(temp_path if 'temp_path' in locals() else "")
        
        # Actionable Teams Alert
        if isinstance(e, ValueError):
            
            # 🚀 1. Build the Data Table (FactSet)
            error_facts = {
                "Rule": rule['rule_name'],
                "Show": rule['metadata'].get('show_name', 'Unknown'),
                "Venue": rule['metadata'].get('venue_name', 'Unknown'),
                "Error Details": str(e)
            }
            
            # 🚀 2. Send the message with the facts (No SharePoint button yet!)
            send_teams_notification(
                message="⚠️ **Action Required: Data Mapping Failed**\n\nPlease update the local lookup CSV on the server and remove the 'sales_report_failed' tag in Outlook to replay.", 
                logger=logger,
                facts=error_facts
            )
        else:
            # Catch-all for other random errors
            send_teams_notification(
                message=f"❌ **Extraction Failed**\n\nAn unexpected error occurred.", 
                logger=logger,
                facts={"Rule": r_name, "Error Type": type(e).__name__, "Details": str(e)}
            )
        
        # 🚀 THE FIX: Tag it as explicitly failed
        try:
            graph.tag_email(msg_id, "sales_report_failed") 
        except Exception as tag_err:
            logger.error(f"Failed to tag email after failure: {tag_err}")
            
        return False, None, r_name

@flow(name="Sales Extractor Flow", log_prints=True)
def sales_extractor_flow(days_back: int = 30, target_rule_name: str | None = None):
    # Pass parameters down to the fetch task
    queued_sales_reports = fetch_and_route_emails(days_back, target_rule_name)
    successful_runs = []
    failed_runs = []
    
    for queued_sales_report in queued_sales_reports:
        success, rec_date, r_name = process_email(queued_sales_report)
        if success:
            successful_runs.append((r_name, rec_date))
        else:
            failed_runs.append(r_name)
    
    # Flow Completion Summary Alert
    if queued_sales_reports:
        logger = get_run_logger()
        summary = (
            f"📊 **Extraction Flow Complete**\n\n"
            f"**Total Queued Sales Reports:** {len(queued_sales_reports)}\n"
            f"**Successful:** {len(successful_runs)}\n"
            f"**Failed:** {len(failed_runs)}"
        )
        logger.info(f"🏁 Flow Summary: {len(successful_runs)} successful, {len(failed_runs)} failed.")
        send_teams_notification(summary, logger)

if __name__ == "__main__":
    sales_extractor_flow.serve(
        name="sales-extractor-flow",
        cron="*/15 * * * *",
        tags=["medallion-raw", "production"],
        description="Automated email extraction. By default, scans a rolling 30-day window. Use 'Custom Run' to perform historical backfill",
        limit=1
    )
