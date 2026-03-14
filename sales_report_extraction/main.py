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
from src.sharepoint_uploader import SharePointUploader

# 1. Setup Environment
setup_environment()

# 2. Load Configuration
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
    start_date_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    
    seen_fingerprints = set()

    for rule in CONFIG['rules']:
        if not rule.get('active'): continue
        if target_rule and rule['rule_name'] != target_rule: continue

        crit = rule['match_criteria']
        search_query = f'"{crit["subject_keyword"]}"'

        emails = graph.search_emails(search_query)
        skipped = 0

        for email in emails:
            email_dt = date_parser.parse(email['receivedDateTime']).astimezone(timezone.utc)
            existing_tags = email.get('categories', [])
            fingerprint = email.get('internetMessageId')
            
            if email_dt < start_date_dt or "sales_report_extracted" in existing_tags or "sales_report_failed" in existing_tags or "sales_report_duplicate" in existing_tags or not email.get('hasAttachments'):
                skipped += 1
                continue

            actual_sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
            if crit['sender_domain'].lower() in actual_sender:
                if fingerprint and fingerprint in seen_fingerprints:
                    logger.info(f"👯 Twin detected: '{email['subject']}'. Tagging as duplicate.")
                    try:
                        graph.tag_email(email['id'], "sales_report_duplicate")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to tag duplicate: {e}")
                    skipped += 1
                    continue
                
                seen_fingerprints.add(fingerprint)
                queued_sales_reports.append({"email_data": email, "rule": rule})
            else:
                skipped += 1
                
        logger.info(f"📊 Rule '{rule['rule_name']}': Found {len(emails)}, Queued {len(emails) - skipped}")

    return queued_sales_reports

@task(name="Process Email Attachment")
def process_email(queued_sales_report, disable_notifications: bool = False):
    sp_uploader = SharePointUploader()  
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
        content_bytes, _ = graph.download_attachment(msg_id, expected_ext)
        std_name = engine.generate_filename(rule['metadata'], email['receivedDateTime'], expected_ext)
        temp_path = os.path.join(engine.base_dir, engine.dirs['inbox'], std_name)
        
        with open(temp_path, 'wb') as f: 
            f.write(content_bytes)
            f.flush()
            os.fsync(f.fileno())

        raw_url = sp_uploader.upload_file(temp_path, std_name, show_name, venue_name, "Raw")
        df, validation_result, csv_path = engine.process_file(temp_path, rule)

        processed_url = None
        if csv_path and os.path.exists(csv_path) and csv_path != temp_path:
            csv_filename = os.path.basename(csv_path)
            processed_url = sp_uploader.upload_file(csv_path, csv_filename, show_name, venue_name, "Processed")
            upload_to_sftp(local_file_path=csv_path, filename=csv_filename)

        md_table = f"## Validation Result: {validation_result.status}\n\n**Message:** {validation_result.message}\n\n| Metric | Value |\n|---|---|\n"
        for k, v in validation_result.metrics.items(): 
            md_table += f"| {k} | {v} |\n"
        create_markdown_artifact(key=f"val-{msg_id[:15].lower()}", markdown=md_table, description=r_name)

        is_passthrough = rule.get('processing', {}).get('passthrough_only', False)
        email_date_str = date_parser.parse(email['receivedDateTime']).strftime('%Y-%m-%d')
        raw_link_md = f"[Raw Attachment]({raw_url})" if raw_url else "Raw Upload Failed"
        
        if is_passthrough:
            link_display = f"📁 {raw_link_md} *(Straight to SFTP)*"
        elif processed_url:
            link_display = f"📁 {raw_link_md}  |  📊 [Processed CSV]({processed_url})"
        else:
            link_display = f"📁 {raw_link_md}"

        graph.tag_email(msg_id, "sales_report_extracted")
        
        return True, r_name, {
            "display": f"{show_name} - {venue_name}",
            "date": email_date_str,
            "links": link_display,
            "needs_review": (validation_result.status == "UNVALIDATED")
        }

    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        engine.handle_failure(temp_path if 'temp_path' in locals() else "")
        
        if isinstance(e, ValueError):
            error_details = str(e)
            is_mapping = any(keyword in error_details.lower() for keyword in ["lookup", "mapping", "code", "unmapped"])
            
            if is_mapping:
                try:
                    from src.error_db_client import log_lookup_failure
                    import re
                    match = re.search(r"\{([^}]+)\}", error_details)
                    missing_code = match.group(1).replace("'", "").strip() if match else error_details[:60]
                    log_lookup_failure(show_name, venue_name, str(rule['metadata'].get('show_id', 'Unknown')), str(rule['metadata'].get('venue_id', 'Unknown')), missing_code, msg_id)
                except Exception as db_err:
                    pass

            if not disable_notifications:
                send_teams_notification(
                    message=f"{'⚠️ **Action Required: Data Mapping Failed**' if is_mapping else '❌ **Action Required: File Parsing Failed**'}\n\n{error_details}", 
                    logger=logger,
                    facts={"Rule": rule['rule_name'], "Show": show_name, "Venue": venue_name},
                    channel="dev"
                )
        else:
            if not disable_notifications:
                send_teams_notification(
                    message=f"❌ **System Error: Extraction Failed**\n\nAn unexpected Python exception occurred.", 
                    logger=logger,
                    facts={"Rule": r_name, "Error Type": type(e).__name__, "Details": str(e)},
                    channel="dev"
                )
        
        try:
            graph.tag_email(msg_id, "sales_report_failed") 
        except Exception:
            pass
            
        return False, r_name, None

@task(name="Reset Failed Emails")
def reset_failed_emails(days_back: int):
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
        reset_count = reset_failed_emails(days_back)
        if reset_count > 0:
            logger.info(f"✅ Wiped failed tag from {reset_count} emails.")

    queued_sales_reports = fetch_and_route_emails(days_back, target_rule_name)
    
    success_list = []
    review_list = []
    failed_count = 0
    
    for queued_sales_report in queued_sales_reports:
        success, r_name, info = process_email(queued_sales_report, disable_notifications)
        
        if success:
            if info["needs_review"]:
                review_list.append(info)
            else:
                success_list.append(info)
        else:
            failed_count += 1
    
    if queued_sales_reports:
        logger = get_run_logger()
        total_processed = len(success_list) + len(review_list)
        logger.info(f"🏁 Flow Summary: {total_processed} successful, {failed_count} failed.")
        
        # 🚀 UPDATED: Formatted tightly with markdown line-breaks (two spaces + \n) and list separators
        if (success_list or review_list) and not disable_notifications:
            msg_parts = [f"📊 **Batch Extraction Complete ({total_processed} Files)**\n"]
            
            if success_list:
                msg_parts.append("\n**✅ Successfully Processed:**\n")
                display_limit = 10
                for item in success_list[:display_limit]:
                    msg_parts.append(f"**{item['display']}** ({item['date']})  \n↳ {item['links']}\n")
                
                if len(success_list) > display_limit:
                    msg_parts.append(f"\n*(...and {len(success_list) - display_limit} more successfully processed. Check SharePoint for full list.)*\n")
                    
            if review_list:
                msg_parts.append("\n**⚠️ Manual Review Required:**\n")
                display_limit = 10
                for item in review_list[:display_limit]:
                    msg_parts.append(f"**{item['display']}** ({item['date']})  \n↳ {item['links']}\n")
                    
                if len(review_list) > display_limit:
                    msg_parts.append(f"\n*(...and {len(review_list) - display_limit} more requiring manual review.)*\n")
                    
            if failed_count > 0:
                msg_parts.append(f"\n❌ *{failed_count} file(s) failed. See Dev channel for details.*")
            
            send_teams_notification(
                message="\n".join(msg_parts), 
                logger=logger,
                channel="ops" 
            )

if __name__ == "__main__":
    sales_extractor_flow.serve(
        name="sales-extractor-flow",
        cron="*/15 * * * *",
        tags=["medallion-raw", "production"],
        description="Automated email extraction. Includes dynamic rule routing, lookup handling, SharePoint uploads, and SFTP delivery."
    )