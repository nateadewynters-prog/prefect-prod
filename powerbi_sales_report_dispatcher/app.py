import os, time, requests, msal, pyodbc, base64, fitz, json
from datetime import datetime, timedelta
from flask import Flask, render_template, Response, stream_with_context
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = "figures@dewynters.com"

# --- CONFIGURATION ---
SHOWS_CONFIG = [
    {
        "id": "1", "show_name": "The Devil Wears Prada", "show_id": 180, "db_type": "Legacy",
        "pbi_workspace_id": "b5687f95-8331-4389-88bc-10680652c6f7", 
        "pbi_report_id": "24784969-474d-4c16-bd45-88a71b8167dd",
        "pbi_dataset_id": "3388428f-e0b7-4d23-b65d-21f77c8d111b", 
        "dashboard_url": "https://app.powerbi.com/groups/b5687f95-8331-4389-88bc-10680652c6f7/reports/24784969-474d-4c16-bd45-88a71b8167dd",
        "recipients": ["figures@dewynters.com"]
    },
    {
        "id": "2", "show_name": "Beetlejuice", "show_id": 281, "db_type": "Legacy",
        "pbi_workspace_id": "9fe3b075-b754-4763-983e-655771e0b7c4", 
        "pbi_report_id": "5d44f020-82c0-46da-938a-b90c6906b079",
        "pbi_dataset_id": "fee1f648-be9b-4d16-b458-df868dee474d",
        "dashboard_url": "https://app.powerbi.com/groups/9fe3b075-b754-4763-983e-655771e0b7c4/reports/5d44f020-82c0-46da-938a-b90c6906b079/0920519f35b44a81ba38",
        "recipients": ["figures@dewynters.com"]
    },
    {
        "id": "3", "show_name": "Mamma Mia!", "show_id": 8, "db_type": "Legacy",
        "pbi_workspace_id": "4900e0ac-9477-4fc1-a82c-6ddc35546023", 
        "pbi_report_id": "00a4bb1a-0691-417e-a94b-f9d09965bf45",
        "pbi_dataset_id": "445be91a-db44-4716-952c-69825afa9270",
        "dashboard_url": "https://app.powerbi.com/groups/4900e0ac-9477-4fc1-a82c-6ddc35546023/reports/00a4bb1a-0691-417e-a94b-f9d09965bf45/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com"]
    },
    {
        "id": "4", "show_name": "Moulin Rouge!", "show_id": 45, "db_type": "Legacy",
        "pbi_workspace_id": "d8e48a79-0972-4f4e-a6da-891f284f7953", 
        "pbi_report_id": "a389ea5b-949f-4bb7-b4f2-97571dee86b3",
        "pbi_dataset_id": "ee878be9-5355-412d-ba52-d4c4c2661cf0",
        "dashboard_url": "https://app.powerbi.com/groups/d8e48a79-0972-4f4e-a6da-891f284f7953/reports/a389ea5b-949f-4bb7-b4f2-97571dee86b3/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com"]
    }
]

# --- HELPERS ---
class LiveReportingEngine:
    def __init__(self):
        self.authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        self.msal_app = msal.ConfidentialClientApplication(CLIENT_ID, authority=self.authority, client_credential=CLIENT_SECRET)
    
    def get_token(self, scopes):
        return self.msal_app.acquire_token_for_client(scopes=scopes).get("access_token")

def fetch_sql_metrics(show_id):
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE=TicketingDS;UID={os.getenv('SQL_USERNAME')};PWD={os.getenv('SQL_PASSWORD')};TrustServerCertificate=yes;"
    yesterday_query = "CAST(DATEADD(day, -1, GETDATE()) AS Date)"
    metrics = {}
    
    with pyodbc.connect(conn_str, timeout=10) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(PerformanceDetailId) FROM PerformanceDetail WHERE ShowId = {show_id} AND CAST(PerformanceDateTime AS Date) = {yesterday_query}")
        metrics['no_of_perfs'] = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT FORMAT(Wrap,'N0'), FORMAT(Tickets,'N0'), FORMAT(SalesATP,'N2'), FORMAT(Advance,'N0'), FORMAT(AdvanceTicketsSales,'N0'), FORMAT(Advance/AdvanceTicketsSales,'N2'), FORMAT(Reserved,'N0'), FORMAT(CumulativeGross,'N0'), FORMAT(CumulativeTicketSales,'N0'), FORMAT(CumulativeGross/CumulativeTicketSales,'N2') FROM CombinedWithEventsView WHERE ShowId = {show_id} AND Wrap IS NOT NULL AND RecordDate = {yesterday_query}")
        metrics['main'] = cursor.fetchone()
        
        cursor.execute(f"WITH ThisWeek AS (SELECT DATEADD(dd, -(DATEPART(dw, MAX(RecordDate))-1), MAX(RecordDate)+1) AS WCDate, DATEADD(dd, 8-(DATEPART(dw, MAX(RecordDate))), MAX(RecordDate)) AS WEDate FROM ChannelSalesView WHERE ShowId = {show_id}) SELECT FORMAT(AVG(PercentageGross)*100,'N0'), FORMAT(AVG(PercentageTicketsSold)*100,'N0') FROM SalesByPerformanceView04 CROSS JOIN ThisWeek WHERE ShowId = {show_id} AND PerformanceDateTime BETWEEN WCDate AND WEDate AND DateOfUpdate = {yesterday_query}")
        metrics['weekly'] = cursor.fetchone()
        
        if metrics['no_of_perfs'] > 0:
            cursor.execute(f"SELECT FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentGrossSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN PercentTicketsSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) < '16:00' THEN Gross/1000 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentGrossSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN PercentTicketsSold * 100 END),'N0'), FORMAT(MAX(CASE WHEN CONVERT(TIME, performancedatetime) >= '16:00' THEN Gross/1000 END),'N0') FROM CombinedWithEventsView WHERE ShowId = {show_id} AND RecordDate = {yesterday_query}")
            metrics['perf_detail'] = cursor.fetchone()
            
    return metrics

def build_email_html(config, m):
    w_g, w_t, w_atp, a_g, a_t, a_atp, res_g, c_g, c_t, c_atp = m['main']
    wk_gp, wk_cap = m['weekly']
    
    def is_val(val):
        return val is not None and str(val).lower() != 'none'

    summary_items = []
    if is_val(w_g) and is_val(w_t):
        summary_items.append(f"&emsp;&bull; Yesterday’s wrap was <strong>£{w_g}</strong> and <strong>{w_t}</strong> tickets with an ATP of <strong>£{w_atp}</strong>.")
    if is_val(c_g) and is_val(c_t):
        summary_items.append(f"&emsp;&bull; Cumulative sales are currently at <strong>£{c_g}</strong> and <strong>{c_t}</strong> tickets with an ATP of <strong>£{c_atp}</strong>.")
    if is_val(a_g) and is_val(a_t):
        summary_items.append(f"&emsp;&bull; The advance is currently at <strong>£{a_g}</strong> and <strong>{a_t}</strong> tickets with an ATP of <strong>£{a_atp}</strong> (incl. comps).")
    if is_val(res_g):
        summary_items.append(f"&emsp;&bull; The reserve gross is currently <strong>£{res_g}</strong>.")

    if m.get('no_of_perfs', 0) > 0 and 'perf_detail' in m:
        m_gp, m_cap, m_gr, e_gp, e_cap, e_gr = m['perf_detail']
        perf_sub_items = []
        if is_val(m_gp):
            perf_sub_items.append(f"&emsp;&emsp;Matinee - {m_gp}% GP (£{m_gr}k) and {m_cap}% capacity.")
        if is_val(e_gp):
            perf_sub_items.append(f"&emsp;&emsp;Evening - {e_gp}% GP (£{e_gr}k) and {e_cap}% capacity.")
        if perf_sub_items:
            summary_items.append("&emsp;&bull; Yesterday’s performances:")
            summary_items.extend(perf_sub_items)

    if is_val(wk_gp) and is_val(wk_cap):
        summary_items.append(f"&emsp;&bull; This week’s performances average <strong>{wk_gp}% GP</strong> and <strong>{wk_cap}% capacity</strong>.")

    summary_html_block = "".join([f'<p style="margin:0;">{item}</p>' for item in summary_items])

    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; color: #000000; line-height: 1.4; font-size: 11pt;">
        <div style="max-width: 850px;">
            <p>Dear all,</p>
            <p>Please find attached your report for <strong>{config['show_name']}</strong><br>
            To view this on the Power BI Dashboard click <a href="{config['dashboard_url']}" style="color: #0078D4; text-decoration: none;">here</a>.</p>
            <p style="margin-bottom: 4px;">In summary:</p>
            {summary_html_block}
            <br>
            <img src="cid:preview_image_001" style="width: 100%; max-width: 800px; border: 1px solid #EEEEEE; display: block;">
            <br>
            <p style="margin:0;">All the best,<br><strong>The Dewynters Team</strong></p>
        </div>
    </body>
    </html>
    """

def send_graph_email(config, html_body, pdf_content, png_bytes, graph_token):
    display_date = (datetime.now() - timedelta(1)).strftime('%a %d/%m/%Y')
    file_date = (datetime.now() - timedelta(1)).strftime('%d_%m_%Y')
    
    payload = {
        "message": {
            "subject": f"{config['show_name']} Sales Report - {display_date}",
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": email}} for email in config['recipients']],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": f"{config['show_name']} Sales Report_{file_date}.pdf",
                    "contentType": "application/pdf",
                    "contentBytes": base64.b64encode(pdf_content).decode(),
                    "isInline": False
                },
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "preview.png",
                    "contentType": "image/png",
                    "contentBytes": base64.b64encode(png_bytes).decode(),
                    "contentId": "preview_image_001",
                    "isInline": True
                }
            ]
        }
    }
    
    send_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    headers = {"Authorization": f"Bearer {graph_token}", "Content-Type": "application/json"}
    requests.post(send_url, headers=headers, json=payload).raise_for_status()

# --- ROUTES ---
@app.route('/')
def dispatcher():
    return render_template('dispatcher.html', shows=SHOWS_CONFIG)

@app.route('/preview/<show_id>')
def preview_email(show_id):
    config = next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)
    if not config:
        return "Show not found", 404
    try:
        metrics = fetch_sql_metrics(config['show_id'])
        return build_email_html(config, metrics)
    except Exception as e:
        return f"Error fetching preview data: {str(e)}", 500

@app.route('/query/<show_id>')
def query_database(show_id):
    config = next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)
    if not config: return {"error": "Show not found"}, 404
    try:
        m = fetch_sql_metrics(config['show_id'])
        res = [
            {"Metric": "Wrap", "Value": f"£{m['main'][0]}"},
            {"Metric": "Tickets Sold", "Value": m['main'][1]},
            {"Metric": "ATP", "Value": f"£{m['main'][2]}"},
            {"Metric": "Advance £", "Value": f"£{m['main'][3]}"},
            {"Metric": "Advance Tix", "Value": m['main'][4]},
            {"Metric": "Reserve £", "Value": f"£{m['main'][6]}"},
            {"Metric": "Cumul Gross £", "Value": f"£{m['main'][7]}"},
            {"Metric": "Weekly GP %", "Value": f"{m['weekly'][0]}%"},
            {"Metric": "Weekly Cap %", "Value": f"{m['weekly'][1]}%"}
        ]
        return {"show": config['show_name'], "data": res}
    except Exception as e: return {"error": str(e)}, 500

@app.route('/stream/<show_id>')
def stream_logs(show_id):
    def generate():
        def msg(text, msg_type="info"): return f'data: {json.dumps({"msg": text, "type": msg_type})}\n\n'
            
        engine = LiveReportingEngine()
        config = next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)
        if not config:
            yield msg("❌ Error: Show not found", "error")
            return

        yield msg(f"🚀 Starting pipeline for {config['show_name']}...")
        yield msg("🔑 Requesting Azure AD Tokens...")
        pbi_token = engine.get_token(["https://analysis.windows.net/powerbi/api/.default"])
        graph_token = engine.get_token(["https://graph.microsoft.com/.default"])
        
        if not pbi_token or not graph_token:
            yield msg("❌ Auth Failed. Check Azure AD Credentials.", "error")
            return

        pbi_headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}

        yield msg("🔄 Triggering Power BI Dataset Refresh...")
        try:
            refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{config['pbi_workspace_id']}/datasets/{config['pbi_dataset_id']}/refreshes"
            requests.post(refresh_url, headers=pbi_headers, json={}).raise_for_status()
            
            status_url = f"https://api.powerbi.com/v1.0/myorg/groups/{config['pbi_workspace_id']}/datasets/{config['pbi_dataset_id']}/refreshes?$top=1"
            while True:
                poll_resp = requests.get(status_url, headers=pbi_headers).json()
                status = poll_resp.get('value', [{}])[0].get('status', 'Unknown')
                yield msg(f"⏳ Refresh Status: {status}...")
                if status == "Completed":
                    yield msg("✅ Dataset Refresh Completed.", "success")
                    break
                elif status == "Failed":
                    yield msg("❌ Power BI Refresh Failed.", "error")
                    return
                time.sleep(5)
        except Exception as e:
            yield msg(f"❌ Refresh API Error: {str(e)}", "error")
            return

        yield msg("🗄️ Fetching Sales Metrics from SQL...")
        try:
            metrics = fetch_sql_metrics(config['show_id'])
            yield msg(f"📊 SQL Data Fetched.")
        except Exception as e:
            yield msg(f"❌ SQL Error: {str(e)}", "error")
            return

        yield msg("📄 Triggering Power BI PDF Export...")
        try:
            export_url = f"https://api.powerbi.com/v1.0/myorg/groups/{config['pbi_workspace_id']}/reports/{config['pbi_report_id']}/ExportTo"
            resp = requests.post(export_url, headers=pbi_headers, json={"format": "PDF"})
            resp.raise_for_status()
            export_id = resp.json().get("id")
            
            poll_export_url = f"https://api.powerbi.com/v1.0/myorg/groups/{config['pbi_workspace_id']}/reports/{config['pbi_report_id']}/exports/{export_id}"
            while True:
                poll = requests.get(poll_export_url, headers=pbi_headers).json()
                status = poll.get("status")
                yield msg(f"⏳ Export Status: {status}...")
                if status == "Succeeded":
                    break
                elif status == "Failed":
                    yield msg("❌ Power BI Export Failed.", "error")
                    return
                time.sleep(5)
                
            yield msg("📥 Downloading PDF File...")
            pdf_bytes = requests.get(f"{poll_export_url}/file", headers=pbi_headers).content
        except Exception as e:
            yield msg(f"❌ Export API Error: {str(e)}", "error")
            return
            
        yield msg("🖼️ Rendering PNG Preview from PDF...")
        try:
            doc = fitz.open("pdf", pdf_bytes)
            pix = doc.load_page(0).get_pixmap(dpi=150)
            png_bytes = pix.tobytes("png")
            doc.close()
        except Exception as e:
            yield msg(f"❌ Rendering Error: {str(e)}", "error")
            return

        yield msg("📧 Dispatching Email via MS Graph...")
        try:
            send_graph_email(config, build_email_html(config, metrics), pdf_bytes, png_bytes, graph_token)
            yield msg(f"✅ SUCCESS: {config['show_name']} report sent.", "success")
        except Exception as e:
            yield msg(f"❌ Graph API Error: {str(e)}", "error")
            return
        
        yield "data: [DONE]\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8002)