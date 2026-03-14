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
from src.sharepoint_uploader import SharePointUploader  # 🚀 NEW: Import the uploader

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
sp_uploader = SharePointUploader()  # 🚀 NEW: Instantiate the uploader

@task(name="Fetch and Route Emails", retries=2)
def fetch_and_route_emails(days_back: int, target_rule: str | None = None):
    logger = get_run_logger()
    queued_sales_reports = []
    start_date_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    start_date_str = start_date_dt.strftime('%Y-%m-%d')
    
    logger.info(f"🔎 Scanning for untagged emails received since {start_date_str}")

    for rule in CONFIG['rules']:
        if not rule.get('active'): continue
        if target_rule and rule['rule_name'] != target_rule: continue

        crit = rule['match_criteria']
        search_query = f'"{crit["subject_keyword"]}"'

        logger.info(f"--- 📡 Searching for Rule: {rule['rule_name']} ---")
        emails = graph.search_emails(search_query)
        skipped = 0

        for email in emails:
            email_dt = date_parser.parse(email['receivedDateTime']).astimezone(timezone.utc)
            existing_tags = email.get('categories', [])
            
            # Skip if older than days_back OR if it already has our success/failure tags OR no attachments
            if email_dt < start_date_dt or "sales_report_extracted" in existing_tags or "sales_report_failed" in existing_tags or not email.get('hasAttachments'):
                skipped += 1
                continue

            actual_sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
            if crit['sender_domain'].lower() in actual_sender:
                queued_sales_reports.append({"email_data": email, "rule": rule})
            else:
                skipped += 1
                
        logger.info(f"📊 Rule '{rule['rule_name']}': Found {len(emails)} total, Skipped {skipped}, Queued Sales Reports {len(emails) - skipped}")

    return queued_sales_reports

@task(name="Process Email Attachment")
def process_email(queued_sales_report, disable_notifications: bool = False):
    logger = get_run_logger()
    email = queued_sales_report['email_data']
    rule = queued_sales_report['rule']
    r_name = rule['rule_name']
    msg_id = email['id']
    expected_ext = rule['match_criteria']['attachment_type'].lower()
    
    show_name = rule['metadata'].get('show_name', 'Unknown')
    venue_name = rule['metadata'].get('venue_name', 'Unknown')

    logger.info(f"🚀 Processing Rule: {r_name} | Subject: {email['subject']}")
    
    try:
        # 1. Download File
        content_bytes, _ = graph.download_attachment(msg_id, expected_ext)
        std_name = engine.generate_filename(rule['metadata'], email['receivedDateTime'], expected_ext)
        temp_path = os.path.join(engine.base_dir, engine.dirs['inbox'], std_name)
        
        with open(temp_path, 'wb') as f: 
            f.write(content_bytes)
            f.flush()
            os.fsync(f.fileno())

        # 🚀 2. Upload RAW original attachment to SharePoint
        sp_uploader.upload_file(temp_path, std_name, show_name, venue_name, "Raw")

        # 3. Process File (Convert to CSV/Handle Passthrough)
        df, validation_result, csv_path = engine.process_file(temp_path, rule)

        # 4. Handle Medallion Exports (Only if it's not a passthrough)
        if csv_path and os.path.exists(csv_path) and csv_path != temp_path:
            csv_filename = os.path.basename(csv_path)
            
            # Send to SFTP
            upload_to_sftp(local_file_path=csv_path, filename=csv_filename)
            
            # 🚀 Send to SharePoint Processed Folder
            sp_uploader.upload_file(csv_path, csv_filename, show_name, venue_name, "Processed")

        # 5. Create Artifacts
        md_table = f"## Validation Result: {validation_result.status}\n\n**Message:** {validation_result.message}\n\n| Metric | Value |\n|---|---|\n"
        for k, v in validation_result.metrics.items(): 
            md_table += f"| {k} | {v} |\n"
            
        create_markdown_artifact(
            key=f"val-{msg_id[:15].lower()}",
            markdown=md_table,
            description=r_name
        )

        # 6. Manual Review Alerting
        if validation_result.status == "UNVALIDATED" and not disable_notifications:
            send_teams_notification(
                message="⚠️ **Manual Review Required**\n\nThe contractual PDF was successfully uploaded, but it requires manual visual validation as there are no internal calculation footers.", 
                logger=logger,
                facts={
                    "Rule": r_name,
                    "Message": validation_result.message
                }
            )

        # Tag as successful in Outlook
        graph.tag_email(msg_id, "sales_report_extracted")
        return True, email['receivedDateTime'], r_name

    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        engine.handle_failure(temp_path if 'temp_path' in locals() else "")
        
        # Determine if it's a mapping error
        if isinstance(e, ValueError):
            error_details = str(e)
            is_mapping = any(keyword in error_details.lower() for keyword in ["lookup", "mapping", "code", "unmapped"])
            
            if is_mapping:
                try:
                    from src.error_db_client import log_lookup_failure
                    import re
                    # Try to extract just the unmapped code
                    match = re.search(r"\{([^}]+)\}", error_details)
                    missing_code = match.group(1).replace("'", "").strip() if match else error_details[:60]
                    
                    log_lookup_failure(
                        show_name=show_name,
                        venue_name=venue_name,
                        show_id=str(rule['metadata'].get('show_id', 'Unknown')),
                        venue_id=str(rule['metadata'].get('venue_id', 'Unknown')),
                        missing_code=missing_code,
                        msg_id=msg_id
                    )
                    logger.info(f"💾 Logged mapping error to DataOps DB: {missing_code}")
                except Exception as db_err:
                    logger.error(f"⚠️ Failed to write to DataOps DB: {db_err}")

            alert_title = "⚠️ **Action Required: Data Mapping Failed**" if is_mapping else "❌ **Action Required: File Parsing Failed**"
            alert_body = "Please map the missing code in the DataOps control center." if is_mapping else "The extraction script rejected this file's formatting."

            if not disable_notifications:
                send_teams_notification(
                    message=f"{alert_title}\n\n{alert_body}", 
                    logger=logger,
                    facts={
                        "Rule": rule['rule_name'],
                        "Show": show_name,
                        "Venue": venue_name,
                        "Error Details": error_details
                    }
                )
        else:
            if not disable_notifications:
                send_teams_notification(
                    message=f"❌ **System Error: Extraction Failed**\n\nAn unexpected Python exception occurred during processing.", 
                    logger=logger,
                    facts={
                        "Rule": r_name,
                        "Error Type": type(e).__name__,
                        "Details": str(e)
                    }
                )
        
        # Tag as failed in Outlook
        try:
            graph.tag_email(msg_id, "sales_report_failed") 
        except Exception:
            pass
            
        return False, None, r_name

@task(name="Reset Failed Emails")
def reset_failed_emails(days_back: int):
    """Untags emails marked as 'sales_report_failed' so they get picked up in the next scan."""
    logger = get_run_logger()
    start_date_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    emails = graph.search_emails('"sales_report_failed"')
    reset_count = 0
    
    for email in emails:
        existing_tags = email.get('categories', [])
        email_dt = date_parser.parse(email['receivedDateTime']).astimezone(timezone.utc)
        if "sales_report_failed" in existing_tags and email_dt >= start_date_dt:
            try:
                graph.untag_email(email['id'], "sales_report_failed")
                reset_count += 1
            except Exception:
                pass
    return reset_count

@flow(name="Sales Extractor Flow", log_prints=True)
def sales_extractor_flow(days_back: int = 30, target_rule_name: str | None = None, retry_failed: bool = False, disable_notifications: bool = False):
    if retry_failed:
        logger = get_run_logger()
        logger.info("♻️ Bulk Retry Enabled: Searching for 'sales_report_failed' emails to reset...")
        reset_count = reset_failed_emails(days_back)
        if reset_count > 0:
            logger.info(f"✅ Successfully wiped the failed tag from {reset_count} emails. They will now be reprocessed.")
        else:
            logger.info("ℹ️ No failed emails found to reset.")

    queued_sales_reports = fetch_and_route_emails(days_back, target_rule_name)
    
    successful_runs = []
    failed_runs = []
    success_breakdown = {}
    failed_breakdown = {}
    
    for queued_sales_report in queued_sales_reports:
        success, rec_date, r_name = process_email(queued_sales_report, disable_notifications)
        
        meta = queued_sales_report['rule']['metadata']
        display_name = f"{meta.get('show_name', 'Unknown')} - {meta.get('venue_name', 'Unknown')}"
        
        if success:
            successful_runs.append((r_name, rec_date))
            success_breakdown[display_name] = success_breakdown.get(display_name, 0) + 1
        else:
            failed_runs.append(r_name)
            failed_breakdown[display_name] = failed_breakdown.get(display_name, 0) + 1
    
    # Flow Completion Summary Alert
    if queued_sales_reports:
        logger = get_run_logger()
        logger.info(f"🏁 Flow Summary: {len(successful_runs)} successful, {len(failed_runs)} failed.")
        
        summary_facts = {
            "Total Queued": len(queued_sales_reports),
            "Successful": len(successful_runs),
            "Failed": len(failed_runs)
        }
        
        for name, count in success_breakdown.items():
            summary_facts[f"✅ {name}"] = f"{count} report(s) extracted"
            
        for name, count in failed_breakdown.items():
            summary_facts[f"❌ {name}"] = f"{count} report(s) failed"

        if not disable_notifications:
            send_teams_notification(
                message="📊 **Extraction Flow Complete**", 
                logger=logger,
                facts=summary_facts
            )

if __name__ == "__main__":
    sales_extractor_flow.serve(
        name="sales-extractor-flow",
        cron="*/15 * * * *",
        tags=["medallion-raw", "production"],
        description="Automated email extraction. Includes dynamic rule routing, lookup handling, SharePoint uploads, and SFTP delivery."
    )