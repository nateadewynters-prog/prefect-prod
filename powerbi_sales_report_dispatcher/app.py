import os, time, requests, msal, pyodbc, base64, fitz, json, sqlite3
from datetime import datetime, timedelta
from flask import Flask, render_template, Response, stream_with_context, jsonify
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = os.getenv("BUSINESS_INTELLIGENCE_INBOX_ADDRESS")

# --- CONFIGURATION ---
SHOWS_CONFIG = [
    {
        "id": "1", "show_name": "The Devil Wears Prada", "show_id": 180, "db_type": "Legacy",
        "pbi_workspace_id": "b5687f95-8331-4389-88bc-10680652c6f7", 
        "pbi_report_id": "24784969-474d-4c16-bd45-88a71b8167dd",
        "pbi_dataset_id": "3388428f-e0b7-4d23-b65d-21f77c8d111b", 
        "dashboard_url": "https://app.powerbi.com/groups/b5687f95-8331-4389-88bc-10680652c6f7/reports/24784969-474d-4c16-bd45-88a71b8167dd",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "2", "show_name": "Beetlejuice", "show_id": 281, "db_type": "Legacy",
        "pbi_workspace_id": "9fe3b075-b754-4763-983e-655771e0b7c4", 
        "pbi_report_id": "5d44f020-82c0-46da-938a-b90c6906b079",
        "pbi_dataset_id": "fee1f648-be9b-4d16-b458-df868dee474d",
        "dashboard_url": "https://app.powerbi.com/groups/9fe3b075-b754-4763-983e-655771e0b7c4/reports/5d44f020-82c0-46da-938a-b90c6906b079/0920519f35b44a81ba38",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "3", "show_name": "Mamma Mia!", "show_id": 8, "db_type": "Legacy",
        "pbi_workspace_id": "4900e0ac-9477-4fc1-a82c-6ddc35546023", 
        "pbi_report_id": "00a4bb1a-0691-417e-a94b-f9d09965bf45",
        "pbi_dataset_id": "445be91a-db44-4716-952c-69825afa9270",
        "dashboard_url": "https://app.powerbi.com/groups/4900e0ac-9477-4fc1-a82c-6ddc35546023/reports/00a4bb1a-0691-417e-a94b-f9d09965bf45/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "4", "show_name": "Moulin Rouge!", "show_id": 45, "db_type": "Legacy",
        "pbi_workspace_id": "d8e48a79-0972-4f4e-a6da-891f284f7953", 
        "pbi_report_id": "a389ea5b-949f-4bb7-b4f2-97571dee86b3",
        "pbi_dataset_id": "ee878be9-5355-412d-ba52-d4c4c2661cf0",
        "dashboard_url": "https://app.powerbi.com/groups/d8e48a79-0972-4f4e-a6da-891f284f7953/reports/a389ea5b-949f-4bb7-b4f2-97571dee86b3/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
    "id": "5", "show_name": "My Neighbour Totoro", "show_id": 222, "db_type": "Legacy",
    "pbi_workspace_id": "2d12753e-740c-421c-b84c-20790dedc4f2",
    "pbi_report_id": "5ba3d957-c0ba-4027-8aea-12730ede5113",
    "pbi_dataset_id": "c14312f5-bd83-44cc-95ef-27ba1b86ddbe",
    "dashboard_url": "https://app.powerbi.com/groups/2d12753e-740c-421c-b84c-20790dedc4f2/reports/5ba3d957-c0ba-4027-8aea-12730ede5113",
    "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "id": "6", "show_name": "Magic Mike", "show_id": 44, "db_type": "TransactLive", # Unique identifier for the new DB
        "pbi_workspace_id": "67ad38b6-3981-401a-9032-2d0807b5f8d6", 
        "pbi_report_id": "c4fa1a2d-7882-4bba-91c8-b8bb1114cdb5",
        "pbi_dataset_id": "2176834f-4728-4e1a-bc23-196b43d70b2d",
        "dashboard_url": "https://app.powerbi.com/groups/67ad38b6-3981-401a-9032-2d0807b5f8d6/reports/c4fa1a2d-7882-4bba-91c8-b8bb1114cdb5",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    }
]

# --- SHARED STATE DATABASE (LOCKS & LOGS) ---
DB_PATH = os.getenv("DB_PATH", "dispatcher_state.db")

def get_db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS locks (show_id TEXT PRIMARY KEY, is_locked INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT, type TEXT, timestamp DATETIME DEFAULT (datetime('now', 'localtime')))")
        conn.execute("CREATE TABLE IF NOT EXISTS dispatch_history (id INTEGER PRIMARY KEY AUTOINCREMENT, show_name TEXT, duration_mins INTEGER, pdf_size_mb REAL, timestamp DATETIME DEFAULT (datetime('now', 'localtime')))")
        conn.execute("UPDATE locks SET is_locked = 0")
init_db()

def set_lock(show_id, locked):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO locks (show_id, is_locked) VALUES (?, ?) ON CONFLICT(show_id) DO UPDATE SET is_locked = ?", (show_id, int(locked), int(locked)))

def is_any_locked():
    with get_db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) as active_locks FROM locks WHERE is_locked = 1").fetchone()
        return row['active_locks'] > 0

def db_log(msg, msg_type="info"):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO logs (msg, type) VALUES (?, ?)", (msg, msg_type))

# --- HELPERS ---
class LiveReportingEngine:
    def __init__(self):
        self.authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        self.msal_app = msal.ConfidentialClientApplication(CLIENT_ID, authority=self.authority, client_credential=CLIENT_SECRET)
    
    def get_token(self, scopes):
        return self.msal_app.acquire_token_for_client(scopes=scopes).get("access_token")

def fetch_legacy_metrics(show_id):
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE=TicketingDS;UID={os.getenv('SQL_USERNAME_BILOGIN')};PWD={os.getenv('SQL_PASSWORD_BILOGIN')};TrustServerCertificate=yes;"
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

def fetch_transact_metrics(show_id):
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE=TransactDSLive;UID={os.getenv('SQL_USERNAME_BILOGIN')};PWD={os.getenv('SQL_PASSWORD_BILOGIN')};TrustServerCertificate=yes;"
    metrics = {}
    
    with pyodbc.connect(conn_str, timeout=10) as conn:
        cursor = conn.cursor()
        
        # 1. Perf Checker
        cursor.execute(f"SELECT DISTINCT(COUNT(PerformanceDetailId)) FROM PerformanceDetail WHERE ShowId = {show_id} AND CAST(PerformanceDateTime AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date)")
        metrics['no_of_perfs'] = cursor.fetchone()[0]
        
        # 2. Wrap
        cursor.execute(f"SELECT FORMAT(SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 THEN Gross ELSE 0 END),'N0'), FORMAT(SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 THEN TicketCount ELSE 0 END),'N0'), CAST(SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 THEN Gross ELSE 0 END)/ SUM(CASE WHEN CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0 AND IsComp = 0 THEN TicketCount ELSE 0 END) AS decimal(5,2)) FROM TicketSale TS WHERE TS.PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id}) AND CAST(TS.PurchaseDate AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) GROUP BY CAST(TS.PurchaseDate AS Date)")
        wrap = cursor.fetchone() or (None, None, None)
        
        # 3. Advance
        cursor.execute(f"SELECT FORMAT(SUM(CASE WHEN IsReservation = 0 THEN Gross ELSE 0 END),'N0'), ROUND(SUM(CASE WHEN IsReservation = 0 THEN Gross ELSE 0 END) / 1000000.0,2), FORMAT(SUM(CASE WHEN IsReservation = 0 THEN TicketCount ELSE 0 END),'N0'), ROUND(SUM(CASE WHEN IsReservation = 0 THEN TicketCount ELSE 0 END)/1000.0,1), CAST(SUM(CASE WHEN IsReservation = 0 THEN Gross ELSE 0 END)/ SUM(CASE WHEN IsReservation = 0 AND IsComp = 0 THEN TicketCount ELSE 0 END) as decimal (5,2)), FORMAT(SUM(CASE WHEN IsReservation = 1 THEN Gross ELSE 0 END),'N0'), FORMAT(SUM(CASE WHEN IsReservation = 1 THEN TicketCount ELSE 0 END),'N0') FROM TicketSale WHERE PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id} AND PerformanceDatetime > GETDATE())")
        adv = cursor.fetchone() or (None, None, None, None, None, None, None)
        
        # 4. Cumulative
        cursor.execute(f"SELECT FORMAT((SUM(Gross)+ 14359011),'N0'), ROUND((SUM(Gross)+ 14359011)/1000000.0,1), FORMAT((SUM(TicketCount) + 204451),'N0'), ROUND((SUM(TicketCount) + 204451)/1000.0,1), CAST( (SUM(Gross)+ 14359011) /(SUM(CASE WHEN IsComp = 0 THEN TicketCount ELSE 0 END) + 204451) as decimal (5,2)) FROM TicketSale WHERE PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id}) AND IsReservation = 0")
        cumul = cursor.fetchone() or (None, None, None, None, None)
        
        # Map to Legacy standard, appending Reserved Tickets (adv[6]) to the end
        metrics['main'] = (
            wrap[0], wrap[1], wrap[2],   
            adv[0], adv[2], adv[4], adv[5], 
            cumul[0], cumul[2], cumul[4], adv[6]  
        )
        
        # 5. Weekly
        cursor.execute(f"WITH A AS (SELECT PerformanceDateTime, SUM(Gross) AS Gross, GrossPotential, SUM(TicketCount) AS TicketCount, SeatingCapacity FROM TicketSale TS JOIN PerformanceDetail PD ON TS.PerformanceDetailId = PD.PerformanceDetailId WHERE TS.PerformanceDetailId IN (SELECT DISTINCT PerformanceDetailId FROM PerformanceDetail WHERE ShowId = {show_id}) AND IsReservation = 0 AND CAST(PerformanceDateTime AS Date) BETWEEN CAST(GETDATE() - DATEPART(dw, GETDATE()-2) AS Date) AND CAST(GETDATE() - DATEPART(dw, GETDATE()-2)+7 AS Date) GROUP BY PerformanceDateTime, GrossPotential, SeatingCapacity) SELECT CAST((SUM(A.Gross) / SUM(A.GrossPotential))*100 AS decimal(4,0)), CAST((SUM(A.TicketCount) *1.0 / SUM(A.SeatingCapacity))*100 AS decimal(4,0)) FROM A")
        metrics['weekly'] = cursor.fetchone() or (None, None)
        
        # 6. Perf Detail
        if metrics['no_of_perfs'] > 0:
            cursor.execute(f"SELECT ROUND(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN Gross END)/1000.0,1), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN Gross END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN GrossPotential END) AS float) *100,'N0'), SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN TicketCount END), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN TicketCount END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 3 THEN SeatingCapacity END) AS float) *100,'N0'), ROUND(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN Gross ELSE 0 END)/1000.0,1), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN Gross END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN GrossPotential END) AS float) *100,'N0'), SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN TicketCount ELSE 0 END), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN TicketCount END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 4 THEN SeatingCapacity END) AS float) *100,'N0'), ROUND(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN Gross ELSE 0 END)/1000.0,1), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN Gross END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN GrossPotential END) AS float) *100,'N0'), SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN TicketCount ELSE 0 END), FORMAT(CAST(SUM(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN TicketCount END) AS float) /CAST(AVG(CASE WHEN PN.TimeOfPerformanceNameId = 7 THEN SeatingCapacity END) AS float) *100,'N0') FROM TicketSale TS JOIN PerformanceDetail PD ON PD.PerformanceDetailId = TS.PerformanceDetailId JOIN TimeOfPerformanceNames PN ON PN.TimeOfPerformanceNameId = PD.TimeOfPerformanceNameId WHERE ShowId = {show_id} AND CAST(PD.PerformanceDateTime AS Date) = CAST(DATEADD(day, -1, GETDATE()) AS Date) AND IsReservation = 0")
            perf = cursor.fetchone()
            if perf:
                metrics['perf_detail'] = (
                    perf[1], perf[3], perf[0],  # Matinee (GP%, Cap%, Gross)
                    perf[5], perf[7], perf[4],  # Evening (GP%, Cap%, Gross)
                    perf[9], perf[11], perf[8]  # Night   (GP%, Cap%, Gross)
                )
                
    return metrics

def get_show_metrics(config):
    """Router function to decouple database logic from the rest of the app."""
    if config.get("db_type") == "TransactLive":
        return fetch_transact_metrics(config["show_id"])
    else:
        return fetch_legacy_metrics(config["show_id"])

def build_email_html(config, m):
    main_data = m.get('main', [])
    w_g = main_data[0] if len(main_data) > 0 else None
    w_t = main_data[1] if len(main_data) > 1 else None
    w_atp = main_data[2] if len(main_data) > 2 else None
    a_g = main_data[3] if len(main_data) > 3 else None
    a_t = main_data[4] if len(main_data) > 4 else None
    a_atp = main_data[5] if len(main_data) > 5 else None
    res_g = main_data[6] if len(main_data) > 6 else None
    c_g = main_data[7] if len(main_data) > 7 else None
    c_t = main_data[8] if len(main_data) > 8 else None
    c_atp = main_data[9] if len(main_data) > 9 else None
    res_t = main_data[10] if len(main_data) > 10 else None

    wk_gp, wk_cap = m.get('weekly', (None, None))
    
    def is_val(val):
        return val is not None and str(val).lower() != 'none'

    summary_items = []
    if is_val(w_g) and is_val(w_t):
        summary_items.append(f"&emsp;&bull; Yesterday’s wrap was <strong>£{w_g}</strong> and <strong>{w_t}</strong> tickets with an ATP of <strong>£{w_atp}</strong>.")
    if is_val(a_g) and is_val(a_t):
        summary_items.append(f"&emsp;&bull; The advance is currently at <strong>£{a_g}</strong> and <strong>{a_t}</strong> tickets with an ATP of <strong>£{a_atp}</strong>.")
    if is_val(c_g) and is_val(c_t):
        summary_items.append(f"&emsp;&bull; Cumulative sales are currently at <strong>£{c_g}</strong> and <strong>{c_t}</strong> tickets with an ATP of <strong>£{c_atp}</strong>.")
    
    if is_val(res_g):
        if is_val(res_t):
            summary_items.append(f"&emsp;&bull; The reserve gross is currently <strong>£{res_g}</strong> and <strong>{res_t}</strong> tickets.")
        else:
            summary_items.append(f"&emsp;&bull; The reserve gross is currently <strong>£{res_g}</strong>.")

    # Dynamic Performances
    if m.get('no_of_perfs', 0) > 0 and 'perf_detail' in m:
        p = m['perf_detail']
        perf_sub_items = []
        
        # Parse Matinee and Evening
        if len(p) >= 6:
            m_gp, m_cap, m_gr, e_gp, e_cap, e_gr = p[0:6]
            if is_val(m_gp):
                perf_sub_items.append(f"&emsp;&emsp;Matinee - {m_gp}% GP (£{m_gr}k) and {m_cap}% capacity.")
            if is_val(e_gp):
                perf_sub_items.append(f"&emsp;&emsp;Evening - {e_gp}% GP (£{e_gr}k) and {e_cap}% capacity.")
        
        # Parse Night (if present)
        if len(p) >= 9:
            n_gp, n_cap, n_gr = p[6:9]
            if is_val(n_gp):
                perf_sub_items.append(f"&emsp;&emsp;Night - {n_gp}% GP (£{n_gr}k) and {n_cap}% capacity.")
                
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

@app.route('/api/history')
def get_history():
    with get_db_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM dispatch_history").fetchone()[0]
        # --- NEW: Added duration_mins and pdf_size_mb to the SELECT statement ---
        history = [dict(row) for row in conn.execute("SELECT show_name, duration_mins, pdf_size_mb, timestamp FROM dispatch_history ORDER BY timestamp DESC LIMIT 50").fetchall()]
    return jsonify({"total": total, "history": history})

@app.route('/api/state')
def get_state():
    with get_db_conn() as conn:
        locks = [row['show_id'] for row in conn.execute("SELECT show_id FROM locks WHERE is_locked = 1").fetchall()]
        thirty_mins_ago = (datetime.now() - timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
        logs = [dict(row) for row in conn.execute("SELECT msg, type, timestamp FROM logs WHERE timestamp >= ? ORDER BY timestamp ASC", (thirty_mins_ago,)).fetchall()]
    return jsonify({"locks": locks, "logs": logs})

@app.route('/preview/<show_id>')
def preview_email(show_id):
    config = next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)
    if not config: return "Show not found", 404
    try:
        # Changed this line to use the router
        metrics = get_show_metrics(config) 
        return build_email_html(config, metrics)
    except Exception as e:
        return f"Error fetching preview data: {str(e)}", 500

@app.route('/query/<show_id>')
def query_database(show_id):
    config = next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)
    if not config: return {"error": "Show not found"}, 404
    try:
        m = get_show_metrics(config)
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
    # Enforce Global System Lock: If *any* show is running, reject the request.
    if is_any_locked():
        def reject():
            yield f'data: {json.dumps({"msg": "❌ System busy: Another report is currently being processed. Please wait until it completes.", "type": "error"})}\n\n'
            yield "data: [DONE]\n\n"
        return Response(stream_with_context(reject()), mimetype='text/event-stream')

    set_lock(show_id, True)

    def generate():
        def msg(text, msg_type="info"): 
            db_log(text, msg_type) 
            return f'data: {json.dumps({"msg": text, "type": msg_type})}\n\n'
            
        try:
            config = next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)
            if not config:
                yield msg("❌ Error: Show not found", "error")
                return

            yield msg(f"========== NEW DISPATCH: {config['show_name'].upper()} ==========", "separator")
            yield msg(f"🚀 Starting pipeline for {config['show_name']}...")

            start_time = time.time()
            
            engine = LiveReportingEngine()
            yield msg("🔑 Requesting Azure AD Tokens...")
            pbi_token = engine.get_token(["https://analysis.windows.net/powerbi/api/.default"])
            graph_token = engine.get_token(["https://graph.microsoft.com/.default"])
            
            if not pbi_token or not graph_token:
                yield msg("❌ Auth Failed. Check Azure AD Credentials.", "error")
                return

            pbi_headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}

            yield msg("🔄 Triggering Power BI Dataset Refresh...")
            try:
                # 1. Define the URLs
                refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{config['pbi_workspace_id']}/datasets/{config['pbi_dataset_id']}/refreshes"
                status_url = f"https://api.powerbi.com/v1.0/myorg/groups/{config['pbi_workspace_id']}/datasets/{config['pbi_dataset_id']}/refreshes?$top=1"
                
                # 2. Attempt to trigger the refresh
                trigger_req = requests.post(refresh_url, headers=pbi_headers, json={})
                
                # 3. Check for the specific "already running" error using the exact keywords from your logs
                if trigger_req.status_code == 400 and ("already executing" in trigger_req.text.lower() or "refreshinprogress" in trigger_req.text.lower()):
                    yield msg("ℹ️ A scheduled refresh is already running. Attaching to it...", "info")
                else:
                    # If it's a real error (e.g., 401 Unauthorized), raise it
                    trigger_req.raise_for_status()
                
                # 4. Enter the polling loop
                while True:
                    poll_req = requests.get(status_url, headers=pbi_headers)
                    poll_req.raise_for_status() 
                    status = poll_req.json().get('value', [{}])[0].get('status', 'Unknown')
                    
                    yield msg(f"⏳ Refresh Status: {status}...")
                    if status == "Completed":
                        yield msg("✅ Dataset Refresh Completed.", "success")
                        break
                    elif status == "Failed":
                        yield msg("❌ Power BI Refresh Failed.", "error")
                        return
                    time.sleep(5)
                    
            except requests.exceptions.HTTPError as e:
                yield msg(f"❌ API Error: {e.response.status_code} - {e.response.text}", "error")
                return
            except Exception as e:
                yield msg(f"❌ Refresh API Error: {str(e)}", "error")
                return

            yield msg("🗄️ Fetching Sales Metrics from SQL...")
            try:
                metrics = get_show_metrics(config)

                if not metrics.get('main'):
                    yield msg(f"⛔ No sales data found for {config['show_name']} — report not sent.", "error")
                    return
                if not metrics.get('weekly'):
                    yield msg(f"⛔ No weekly performance data found for {config['show_name']} — report not sent.", "error")
                    return
                    
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
                    poll_req = requests.get(poll_export_url, headers=pbi_headers)
                    poll_req.raise_for_status()
                    status = poll_req.json().get("status")
                    
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
                
                email_list = ", ".join(config['recipients'])
                yield msg(f"Sent to email addresses: {email_list}", "info")
                
                # --- NEW: Calculate and insert metrics ---
                duration_mins = max(1, round((time.time() - start_time) / 60))
                pdf_size_mb = round(len(pdf_bytes) / (1024 * 1024), 2)

                with get_db_conn() as conn:
                    conn.execute(
                        "INSERT INTO dispatch_history (show_name, duration_mins, pdf_size_mb) VALUES (?, ?, ?)", 
                        (config['show_name'], duration_mins, pdf_size_mb)
                    )
                    conn.commit()
                
            except Exception as e:
                yield msg(f"❌ Graph API Error: {str(e)}", "error")
                return
            
            yield "data: [DONE]\n\n"
            
        finally:
            set_lock(show_id, False)

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8002)