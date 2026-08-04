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
from src.config_loader import SharePointRuleLoader
from src.sharepoint_uploader import SharePointUploader

# 1. Setup Environment
setup_environment()

# 2. Load Configuration
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_ROOT, "config", "show_reporting_rules.json")

with open(CONFIG_PATH, 'r') as f:
    CONFIG = json.load(f)

RULES_LIST_NAME = os.getenv("SHAREPOINT_RULES_LIST_NAME", "Sales Reporting - Master List - Automation - Python")
rule_loader = SharePointRuleLoader(RULES_LIST_NAME)

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

    all_rules = rule_loader.load_rules()   # every rule now comes from SharePoint

    queued_sales_reports = []
    start_date_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    seen_fingerprints = set()

    for rule in all_rules:                 # was: for rule in CONFIG['rules']:
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
            
            is_link_based = crit.get('attachment_source') == 'html_link'
            has_physical_attachment = email.get('hasAttachments')

            if email_dt < start_date_dt or "sales_report_extracted" in existing_tags or "sales_report_failed" in existing_tags or "sales_report_duplicate" in existing_tags:
                skipped += 1
                continue
                
            # If it's a standard rule but has no physical attachment, skip it.
            if not is_link_based and not has_physical_attachment:
                skipped += 1
                continue

            actual_sender = email.get('from', {}).get('emailAddress', {}).get('address', '').lower()
            if crit['sender_domain'].lower() in actual_sender:
                # Key on (rule, message) so two rules can each claim the same
                # email when it carries an attachment for each of them.
                if fingerprint and (rule['rule_name'], fingerprint) in seen_fingerprints:
                    logger.info(f"👯 Twin detected: '{email['subject']}'. Tagging as duplicate.")
                    try:
                        graph.tag_email(email['id'], "sales_report_duplicate")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to tag duplicate: {e}")
                    skipped += 1
                    continue
                
                seen_fingerprints.add((rule['rule_name'], fingerprint))
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

    # 🧪 Test rules keep every normal step, but the filename gains a TEST.
    # prefix and delivery goes to the test SFTP account.
    test_mode = rule.get('test', False)

    logger.info(f"🚀 Processing Rule: {r_name} | Subject: {email['subject']}")
    
    try:
        # 🕒 NEW: Sales Day Offset Logic (Adding hours to push late-night emails into tomorrow)
        offset_hours = rule['metadata'].get('sales_day_offset_hours', 0)
        raw_dt = date_parser.parse(email['receivedDateTime'])
        effective_dt = raw_dt + timedelta(hours=offset_hours)
        effective_dt_str = effective_dt.isoformat()
        
        # Calculate standard path FIRST
        std_name = engine.generate_filename(rule['metadata'], effective_dt_str, expected_ext, test_mode)
        temp_path = os.path.join(engine.base_dir, engine.dirs['inbox'], std_name)
        
        attachment_source = rule['match_criteria'].get('attachment_source', 'physical')
        
        if attachment_source == 'html_link':
            from src.link_extractor import download_from_html_link
            html_body = email.get('body', {}).get('content', '')
            download_from_html_link(html_body, temp_path) 
        else:
            filename_keyword = rule['match_criteria'].get('filename_keyword')
            content_bytes, _ = graph.download_attachment(
                msg_id, expected_ext, filename_keyword=filename_keyword
            )
            with open(temp_path, 'wb') as f: 
                f.write(content_bytes)
                f.flush()
                os.fsync(f.fileno())

        raw_url = sp_uploader.upload_file(temp_path, std_name, show_name, venue_name, "Raw")
        df, validation_result, csv_path = engine.process_file(temp_path, rule)

        is_passthrough = rule.get('processing', {}).get('passthrough_only', False)
        processed_url = None
        
        if csv_path and os.path.exists(csv_path) and csv_path != temp_path:
            csv_filename = os.path.basename(csv_path)
            
            # 🚀 THE FIX: Only push to the 'Processed' SharePoint folder if it was actually parsed
            if not is_passthrough:
                processed_url = sp_uploader.upload_file(csv_path, csv_filename, show_name, venue_name, "Processed")
            
            # Passthrough files AND parsed CSVs both go to the contractor via SFTP
            upload_to_sftp(local_file_path=csv_path, filename=csv_filename, test_mode=test_mode)

        # 🏷️ Tag as soon as delivery is done, before the bookkeeping below. The
        # email category is the only durable record that this report has been
        # sent, so anything that throws between the SFTP upload and this line
        # leaves the email unmarked and the next run delivers the same file
        # again. tag_email can also return False without raising, so check it:
        # reporting success on an untagged email guarantees a re-delivery.
        if not graph.tag_email(msg_id, "sales_report_extracted"):
            raise RuntimeError(
                f"Delivered {r_name} but could not tag the email as extracted. "
                f"Failing loudly - leaving it untagged would re-deliver the same "
                f"file to the contractor on the next run."
            )

        md_table = f"## Validation Result: {validation_result.status}\n\n**Message:** {validation_result.message}\n\n| Metric | Value |\n|---|---|\n"
        for k, v in validation_result.metrics.items(): 
            md_table += f"| {k} | {v} |\n"
        create_markdown_artifact(key=f"val-{msg_id[:15].lower()}", markdown=md_table, description=r_name)
        email_date_str = effective_dt.strftime('%Y-%m-%d')
        raw_link_md = f"[Raw Attachment]({raw_url})" if raw_url else "Raw Upload Failed"
        
        if is_passthrough:
            link_display = f"📁 {raw_link_md} *(Straight to SFTP)*"
        elif processed_url:
            link_display = f"📁 {raw_link_md}  |  📊 [Processed CSV]({processed_url})"
        else:
            link_display = f"📁 {raw_link_md}"

        try:
            rule_loader.update_last_run(rule.get("_sp_item_id"))
        except Exception as e:
            logger.warning(f"⚠️ Could not update LastRun for {r_name}: {e}")
        
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
def sales_extractor_flow(days_back: int = 7, target_rule_name: str | None = None, retry_failed: bool = False, disable_notifications: bool = False):
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
        # One run at a time. A run currently takes ~4 of its 15 minutes, but the
        # cost grows with the rule count and rules are added via SharePoint with
        # no code change. Once a run overruns the interval, the next one starts
        # alongside it, sees the same un-tagged emails and delivers every file
        # to the contractor twice. This also makes a post-outage backlog of
        # 'Late' runs drain one at a time instead of all at once.
        limit=1,
        tags=["medallion-raw", "production"],
        description="Automated email extraction. Includes dynamic rule routing, lookup handling, SharePoint uploads, and SFTP delivery."
    )