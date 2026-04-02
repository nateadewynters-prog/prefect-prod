import os
import json
import time
import requests
import msal
import pyodbc
import base64
import fitz  # PyMuPDF
from datetime import datetime, timedelta
from typing import Optional

from prefect import flow, task, get_run_logger

# --- Imports from our local src/ ---
from src.env_setup import setup_environment
from src.templates import SQL_PERF_COUNT, SQL_MAIN_STATS, SQL_WEEKLY_STATS, SQL_MAT_EVE_STATS, get_html_body

# Setup Env & Constants
setup_environment()
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = os.getenv("FIGURES_INBOX_ADDRESS", "figures@dewynters.com")

# The default fallback list if no custom emails are provided
TARGET_RECIPIENTS = [
    "businessintelligence@dewynters.com",
    "figures@dewynters.com"
]

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_ROOT, "config", "shows_config.json")

# --- AUTH HELPER ---
def get_msal_token(scopes):
    app = msal.ConfidentialClientApplication(CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}", client_credential=CLIENT_SECRET)
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" in result: return result["access_token"]
    raise Exception(f"Auth Failed: {result.get('error_description')}")

# --- PREFECT TASKS ---

@task(name="Query SQL Data Warehouse", retries=5, retry_delay_seconds=10, timeout_seconds=60)
def fetch_metrics(config):
    logger = get_run_logger()
    logger.info(f"Connecting to SQL Server ({config['db_type']})...")
    
    server = os.getenv('SQL_SERVER', 'dewyntersticketsql.database.windows.net')
    database = "TicketingDS" 
    user = os.getenv('SQL_USERNAME', 'insightlogin@dewyntersticketsql.database.windows.net')
    pwd = os.getenv('SQL_PASSWORD', 'Fd2CxqEfxVxy7y')
        
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={pwd}'
    metrics = {}
    show_id = config['show_id']
    
    # Calculate exactly "yesterday" in Python formatted for SQL (YYYY-MM-DD)
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        
        # Pass variables cleanly as a tuple: (show_id, target_date)
        cursor.execute(SQL_PERF_COUNT, (show_id, target_date))
        metrics['no_of_perfs'] = cursor.fetchone()[0]
        
        cursor.execute(SQL_MAIN_STATS, (show_id, target_date))
        metrics['main'] = cursor.fetchone()
        
        # Note: SQL_WEEKLY_STATS requires the show_id twice! 
        cursor.execute(SQL_WEEKLY_STATS, (show_id, show_id, target_date))
        metrics['weekly'] = cursor.fetchone()
        
        if metrics['no_of_perfs'] > 0:
            cursor.execute(SQL_MAT_EVE_STATS, (show_id, target_date))
            metrics['perf_detail'] = cursor.fetchone()
            
    return metrics

@task(name="Render PBI PDF", retries=5, retry_delay_seconds=120, timeout_seconds=500)
def export_pbi_pdf(workspace_id, report_id):
    logger = get_run_logger()
    logger.info("Requesting PDF Export from Power BI API (Max 500s timeout)...")
    
    token = get_msal_token(["https://analysis.windows.net/powerbi/api/.default"])
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/ExportTo"
    
    resp = requests.post(url, headers=headers, json={"format": "PDF"})
    resp.raise_for_status()
    export_id = resp.json().get("id")
    status_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/exports/{export_id}"
    
    while True:
        poll = requests.get(status_url, headers=headers).json()
        status = poll.get("status")
        if status == "Succeeded":
            return requests.get(f"{status_url}/file", headers=headers).content
        elif status == "Failed":
            raise Exception("Power BI Export API returned 'Failed'.")
        time.sleep(5)

@task(name="Slice PNG Preview")
def generate_image_preview(pdf_content):
    with fitz.open(stream=pdf_content, filetype="pdf") as doc:
        pix = doc.load_page(0).get_pixmap(matrix=fitz.Matrix(2, 2))
        return pix.tobytes("png")

@task(name="Dispatch Graph Email", timeout_seconds=30)
def send_email(config, metrics, pdf_content, png_bytes, final_recipients):
    """Note: final_recipients parameter dynamically accepts the target email list"""
    logger = get_run_logger()
    
    display_date = (datetime.now() - timedelta(1)).strftime('%a %d/%m/%Y')
    file_date = (datetime.now() - timedelta(1)).strftime('%d_%m_%Y')
    
    w_g, w_t, w_atp, a_g, a_t, a_atp, res_g, c_g, c_t, c_atp = metrics['main']
    wk_gp, wk_cap = metrics['weekly']
    
    perf_section = ""
    if metrics.get('no_of_perfs', 0) > 0 and 'perf_detail' in metrics:
        m_gp, m_cap, m_gr, e_gp, e_cap, e_gr = metrics['perf_detail']
        perf_section = '<p style="margin:0;">&emsp;&bull; Yesterday’s performances:</p>'
        if m_gp is not None: perf_section += f'\n                <p style="margin:0;">&emsp;&emsp;Matinee - {m_gp}% GP (£{m_gr}k) and {m_cap}% capacity.</p>'
        if e_gp is not None: perf_section += f'\n                <p style="margin:0;">&emsp;&emsp;Evening - {e_gp}% GP (£{e_gr}k) and {e_cap}% capacity.</p>'

    html_body = get_html_body(config, w_g, w_t, w_atp, a_g, a_t, a_atp, res_g, c_g, c_t, c_atp, wk_gp, wk_cap, perf_section)
    
    token = get_msal_token(["https://graph.microsoft.com/.default"])
    
    payload = {
        "message": {
            "subject": f"{config['show_name']} Sales Report - {display_date}",
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": email}} for email in final_recipients],
            "attachments": [
                {"@odata.type": "#microsoft.graph.fileAttachment", "name": f"{config['show_name']} Sales Report_{file_date}.pdf", "contentType": "application/pdf", "contentBytes": base64.b64encode(pdf_content).decode(), "isInline": False},
                {"@odata.type": "#microsoft.graph.fileAttachment", "name": "preview.png", "contentType": "image/png", "contentBytes": base64.b64encode(png_bytes).decode(), "contentId": "preview_image_001", "isInline": True}
            ]
        }
    }
    
    send_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    response = requests.post(send_url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=payload)
    response.raise_for_status()
    logger.info(f"✅ Email Dispatched to {len(final_recipients)} recipient(s)!")


# --- MASTER ORCHESTRATOR ---

@flow(name="Outbound PBI Sales Reporting", log_prints=True)
def pbi_sales_reporting_flow(target_show_id: Optional[int] = None, custom_emails: Optional[str] = None):
    """
    Extracts Data, Renders PDF, and Emails. 
    Parameters:
    - target_show_id: Leave blank to run all shows, or provide an ID for a single show.
    - custom_emails: Comma-separated list of emails. Overrides the default recipient list.
    """
    logger = get_run_logger()
    
    # --- DEFENSIVE CATCH ---
    # Quick Runs sometimes pass empty strings ("") instead of Python's 'None'.
    # This guarantees we cast it correctly before our logic runs.
    if target_show_id == "":
        target_show_id = None
    elif target_show_id is not None:
        target_show_id = int(target_show_id)
        
    # 1. Determine Recipients
    if custom_emails:
        # Splits by comma and safely removes any accidental whitespace
        run_recipients = [email.strip() for email in custom_emails.split(',') if email.strip()]
        logger.info(f"📧 Overriding defaults. Sending to custom list: {run_recipients}")
    else:
        run_recipients = TARGET_RECIPIENTS
        
    # 2. Load Config
    with open(CONFIG_PATH, 'r') as f:
        shows_config = json.load(f)

    configs_to_run = shows_config
    if target_show_id:
        configs_to_run = [c for c in shows_config if c['show_id'] == target_show_id]
        if not configs_to_run:
            logger.error(f"❌ No show found with ID {target_show_id}. Exiting.")
            return

    logger.info(f"🚀 Starting Report Batch for {len(configs_to_run)} show(s).")

    # 3. Execute Loop
    for config in configs_to_run:
        logger.info(f"--- 🎭 Processing: {config['show_name']} ---")
        try:
            metrics = fetch_metrics(config)
            pdf_bytes = export_pbi_pdf(config['pbi_workspace_id'], config['pbi_report_id'])
            png_bytes = generate_image_preview(pdf_bytes)
            
            # Pass the dynamically calculated recipient list to the email task
            send_email(config, metrics, pdf_bytes, png_bytes, run_recipients)
            
        except Exception as e:
            logger.error(f"❌ Pipeline Failed for {config['show_name']}: {str(e)}")

if __name__ == "__main__":
    pbi_sales_reporting_flow.serve(
        name="daily-email-dispatch",
        cron="0 10,13,15 * * *", 
        tags=["reporting", "outbound", "power-bi"],
        description="Extracts SQL metrics, exports PBI dashboard, and emails stakeholders."
    )