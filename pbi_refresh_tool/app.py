import streamlit as st
import streamlit.components.v1 as components
import json
import msal
import httpx
import asyncio
import os
import re
import pyodbc
import pandas as pd
import requests
import base64
import time
import fitz  # PyMuPDF
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- CONFIG & DATA ---
load_dotenv()
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = "figures@dewynters.com"
JSON_PATH = './data/PowerBI_Report_IDs.json'

# Database Configuration
DB_CONFIG = {
    'server': os.getenv("SQL_SERVER"),
    'database': 'TicketingDS',
    'username': os.getenv("SQL_USERNAME"),
    'password': os.getenv("SQL_PASSWORD"),
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# Automated Email Configuration
SHOWS_CONFIG = [
    {
        "show_name": "The Devil Wears Prada", "show_id": 180, "db_type": "legacy",
        "pbi_workspace_id": "b5687f95-8331-4389-88bc-10680652c6f7", "pbi_report_id": "24784969-474d-4c16-bd45-88a71b8167dd",
        "dashboard_url": "https://app.powerbi.com/groups/b5687f95-8331-4389-88bc-10680652c6f7/reports/24784969-474d-4c16-bd45-88a71b8167dd",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com"]
    },
    {
        "show_name": "Beetlejuice", "show_id": 281, "db_type": "legacy",
        "pbi_workspace_id": "9fe3b075-b754-4763-983e-655771e0b7c4", "pbi_report_id": "5d44f020-82c0-46da-938a-b90c6906b079",
        "dashboard_url": "https://app.powerbi.com/groups/9fe3b075-b754-4763-983e-655771e0b7c4/reports/5d44f020-82c0-46da-938a-b90c6906b079/0920519f35b44a81ba38",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com"]
    },
    {
        "show_name": "Mamma Mia!", "show_id": 8, "db_type": "legacy",
        "pbi_workspace_id": "4900e0ac-9477-4fc1-a82c-6ddc35546023", "pbi_report_id": "00a4bb1a-0691-417e-a94b-f9d09965bf45",
        "dashboard_url": "https://app.powerbi.com/groups/4900e0ac-9477-4fc1-a82c-6ddc35546023/reports/00a4bb1a-0691-417e-a94b-f9d09965bf45/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    },
    {
        "show_name": "Moulin Rouge!", "show_id": 45, "db_type": "legacy",
        "pbi_workspace_id": "d8e48a79-0972-4f4e-a6da-891f284f7953", "pbi_report_id": "a389ea5b-949f-4bb7-b4f2-97571dee86b3",
        "dashboard_url": "https://app.powerbi.com/groups/d8e48a79-0972-4f4e-a6da-891f284f7953/reports/a389ea5b-949f-4bb7-b4f2-97571dee86b3/80a435e098a8b67d5307",
        "recipients": ["figures@dewynters.com", "a.trott@dewynters.com", "c.dobson@dewynters.com"]
    }
]

st.set_page_config(page_title="PBI & Data Tool", page_icon="📊", layout="wide")

# Initialize Session States
if "is_refreshing" not in st.session_state: st.session_state.is_refreshing = False
if "active_report" not in st.session_state: st.session_state.active_report = None
if "refresh_complete" not in st.session_state: st.session_state.refresh_complete = False

if "email_is_running" not in st.session_state: st.session_state.email_is_running = False
if "email_completed" not in st.session_state: st.session_state.email_completed = False

# --- HELPERS ---
def normalize_text(text):
    if not text: return ""
    return re.sub(r'[^a-zA-Z0-9]', '', str(text)).lower()

def get_token(scopes=["https://analysis.windows.net/powerbi/api/.default"]):
    try:
        authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        app = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
        result = app.acquire_token_for_client(scopes=scopes)
        return result.get("access_token")
    except Exception: return None

@st.cache_data
def fetch_doc_id_data():
    conn_str = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
    query = "SELECT ShowName, TheatreName, DocumentName, ShowId, TheatreId, DocumentTypeId FROM [dbo].[DocumentsAndVenues] ORDER BY ShowName;"
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn)
            for col in ['ShowId', 'TheatreId', 'DocumentTypeId']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            return df.drop_duplicates(subset=['ShowId', 'TheatreId', 'DocumentTypeId'])
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()

@st.cache_data
def load_pbi_report_data():
    if not os.path.exists(JSON_PATH): return pd.DataFrame()
    with open(JSON_PATH, 'r') as f:
        df = pd.DataFrame(json.load(f))
    return df.dropna(subset=['Underlying Dataset ID']).sort_values(by='Report Name')

# --- POWER BI API CALLS ---
async def trigger_pbi_refresh(workspace_id, dataset_id, headers):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    async with httpx.AsyncClient(timeout=30.0) as client:
        return await client.post(url, headers=headers, json={})

async def check_pbi_status(workspace_id, dataset_id, headers):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)
        return resp.json().get('value', [{}])[0] if resp.status_code == 200 else {}

# --- EMAIL REPORTING LOGIC ---
def fetch_metrics(config):
    conn_str = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
    yesterday_query = "CAST(DATEADD(day, -1, GETDATE()) AS Date)"
    metrics = {}
    
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        show_id = config['show_id']
        
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

def export_pbi_pdf(workspace_id, report_id, status_container):
    token = get_token(["https://analysis.windows.net/powerbi/api/.default"])
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
            status_container.write("✅ Power BI rendering complete. Downloading PDF...")
            return requests.get(f"{status_url}/file", headers=headers).content
        elif status == "Failed":
            raise Exception("Power BI Export API returned 'Failed'.")
        
        status_container.write(f"⏳ Waiting for Power BI Export... Status: {status}")
        time.sleep(5)

def build_email_body(config, m):
    w_g, w_t, w_atp, a_g, a_t, a_atp, res_g, c_g, c_t, c_atp = m['main']
    wk_gp, wk_cap = m['weekly']
    
    perf_section = ""
    if m.get('no_of_perfs', 0) > 0 and 'perf_detail' in m:
        m_gp, m_cap, m_gr, e_gp, e_cap, e_gr = m['perf_detail']
        perf_section = '<p style="margin:0;">&emsp;&bull; Yesterday’s performances:</p>'
        if m_gp is not None:
            perf_section += f'\n                <p style="margin:0;">&emsp;&emsp;Matinee - {m_gp}% GP (£{m_gr}k) and {m_cap}% capacity.</p>'
        if e_gp is not None:
            perf_section += f'\n                <p style="margin:0;">&emsp;&emsp;Evening - {e_gp}% GP (£{e_gr}k) and {e_cap}% capacity.</p>'

    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; color: #000000; line-height: 1.4; font-size: 11pt;">
        <div style="max-width: 850px;">
            <p>Dear all,</p>
            <p>Please find attached your report for <strong>{config['show_name']}</strong><br>
            To view this on the Power BI Dashboard click <a href="{config['dashboard_url']}" style="color: #0078D4; text-decoration: none;">here</a>.</p>
            
            <p style="margin:0;">In summary:</p>
            <p style="margin:0;">&emsp;&bull; Yesterday’s wrap was <strong>£{w_g}</strong> and <strong>{w_t}</strong> tickets with an ATP of <strong>£{w_atp}</strong>.</p>
            <p style="margin:0;">&emsp;&bull; Cumulative sales are currently at <strong>£{c_g}</strong> and <strong>{c_t}</strong> tickets with an ATP of <strong>£{c_atp}</strong>.</p>
            <p style="margin:0;">&emsp;&bull; The advance is currently at <strong>£{a_g}</strong> and <strong>{a_t}</strong> tickets with an ATP of <strong>£{a_atp}</strong> (incl. comps).</p>
            <p style="margin:0;">&emsp;&bull; The reserve gross is currently <strong>£{res_g}</strong>.</p>
            {perf_section}
            <p style="margin:0;">&emsp;&bull; This week’s performances average <strong>{wk_gp}% GP</strong> and <strong>{wk_cap}% capacity</strong>.</p>
            
            <br>
            <img src="cid:preview_image_001" style="width: 100%; max-width: 800px; border: 1px solid #EEEEEE; display: block;">
            <br>
            <p style="margin:0;">All the best,<br><strong>The Dewynters Team</strong></p>
        </div>
    </body>
    </html>
    """
    return body

def send_graph_email(config, html_body, pdf_content, png_bytes):
    token = get_token(["https://graph.microsoft.com/.default"])
    if not token:
        raise Exception("Failed to retrieve Microsoft Graph token.")
        
    display_date = (datetime.now() - timedelta(1)).strftime('%a %d/%m/%Y')
    file_date = (datetime.now() - timedelta(1)).strftime('%d_%m_%Y')
    
    pdf_b64 = base64.b64encode(pdf_content).decode()
    png_b64 = base64.b64encode(png_bytes).decode()
    
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
                    "contentBytes": pdf_b64,
                    "isInline": False
                },
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "preview.png",
                    "contentType": "image/png",
                    "contentBytes": png_b64,
                    "contentId": "preview_image_001",
                    "isInline": True
                }
            ]
        }
    }
    
    send_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    response = requests.post(send_url, headers=headers, json=payload)
    response.raise_for_status()

# --- HEADER ---
@st.fragment(run_every=10)
def connection_status_header():
    c1, c2 = st.columns([4, 1])
    with c2:
        token = get_token()
        color = "#10b981" if token else "#ef4444"
        label = "Connected 🟢" if token else "Disconnected 🔴"
        st.markdown(f"<div style='text-align: right;'><span style='color: {color}; font-weight: bold; font-size: 0.85rem;'>{label}</span><br><span style='color: #94a3b8; font-size: 0.7rem;'>Last Checked: {datetime.now().strftime('%H:%M:%S')}</span></div>", unsafe_allow_html=True)

connection_status_header()

# --- NAVIGATION ---
st.sidebar.title("🛠️ Tools")
page = st.sidebar.radio("Go to:", ["Dashboard Refresher", "Automated Email Sender", "DocID Tool"])

# --- PAGE 1: DASHBOARD REFRESHER ---
if page == "Dashboard Refresher":
    st.title("📊 Power BI Refresh Center")
    
    if st.session_state.refresh_complete:
        if st.button("🧹 Clear Status & Continue", use_container_width=True):
            st.session_state.is_refreshing = False
            st.session_state.active_report = None
            st.session_state.refresh_complete = False
            st.rerun()

    if st.session_state.is_refreshing and not st.session_state.refresh_complete:
        st.error(f"🚫 **System Busy:** Currently refreshing **{st.session_state.active_report}**. Controls are locked.")

    df_reports = load_pbi_report_data()
    
    if not df_reports.empty:
        selected_name = st.selectbox(
            "Select Report to Refresh:", 
            df_reports['Report Name'].tolist(), 
            index=None, 
            placeholder="Search reports...",
            disabled=st.session_state.is_refreshing
        )
        
        if selected_name:
            details = df_reports[df_reports['Report Name'] == selected_name].iloc[0]
            col_a, col_b = st.columns(2)
            with col_a:
                report_url = f"https://app.powerbi.com/groups/{details['Workspace ID']}/reports/{details['Report ID']}"
                st.link_button(f"🔗 View {selected_name}", report_url, use_container_width=True, disabled=st.session_state.is_refreshing)
            with col_b:
                if st.button("🚀 Trigger Refresh", use_container_width=True, type="primary", disabled=st.session_state.is_refreshing):
                    st.session_state.is_refreshing = True
                    st.session_state.active_report = selected_name
                    st.session_state.refresh_complete = False
                    st.rerun()

    # Monitoring Engine
    if st.session_state.is_refreshing and st.session_state.active_report:
        st.divider()
        st.subheader(f"Monitoring: {st.session_state.active_report}")
        
        token = get_token()
        if token:
            headers = {"Authorization": f"Bearer {token}"}
            start_time = time.time()
            p_bar = st.progress(0, text="Initializing API handshake...")
            log_container = st.expander("Live Refresh Log", expanded=True)
            
            if not st.session_state.refresh_complete:
                with st.status(f"Communicating with Power BI Service...", expanded=True) as status_box:
                    log_container.write(f"[{datetime.now().strftime('%H:%M:%S')}] Attempting to trigger {st.session_state.active_report}...")
                    response = asyncio.run(trigger_pbi_refresh(details['Workspace ID'], details['Underlying Dataset ID'], headers))
                    
                    if response.status_code == 202:
                        log_container.write(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Success: Request accepted.")
                        p_bar.progress(25, text="In Queue...")
                        
                        completed = False
                        while not completed:
                            data = asyncio.run(check_pbi_status(details['Workspace ID'], details['Underlying Dataset ID'], headers))
                            pbi_status = data.get('status', 'Unknown')
                            elapsed = int(time.time() - start_time)
                            
                            if pbi_status == "InProgress":
                                p_bar.progress(60, text=f"Processing... ({elapsed}s)")
                                status_box.update(label=f"🔄 Processing... {elapsed}s", state="running")
                            
                            log_container.write(f"[{datetime.now().strftime('%H:%M:%S')}] Status: **{pbi_status}**")
                            
                            if pbi_status == "Completed":
                                p_bar.progress(100, text="Dataset Updated!")
                                st.success(f"✅ **Success:** {st.session_state.active_report} refreshed successfully.")
                                st.balloons()
                                st.session_state.refresh_complete = True
                                completed = True
                                st.rerun() 
                            elif pbi_status == "Failed":
                                p_bar.progress(100, text="Failed.")
                                st.error(f"❌ **Error:** The refresh for {st.session_state.active_report} failed.")
                                st.session_state.refresh_complete = True
                                completed = True
                                st.rerun()
                            else:
                                asyncio.run(asyncio.sleep(8))
                    else:
                        st.error(f"API Error {response.status_code}")
                        st.session_state.refresh_complete = True
            else:
                st.info("Refresh process finished. Review logs above and click 'Clear Status' to continue.")

# --- PAGE 2: AUTOMATED EMAIL SENDER ---
elif page == "Automated Email Sender":
    st.title("✉️ Power BI Email Automated Send")
    st.markdown("Select a show to extract SQL metrics, generate a Power BI PDF report, build an email preview, and dispatch it to the designated recipients.")
    
    # Clearance Logic
    if st.session_state.email_completed:
        if st.button("🧹 Clear Status & Continue", use_container_width=True):
            st.session_state.email_is_running = False
            st.session_state.email_completed = False
            st.rerun()
            
    if st.session_state.email_is_running and not st.session_state.email_completed:
        st.error("🚫 **System Busy:** Email pipeline is currently running. Please wait for completion.")
    
    show_options = [show["show_name"] for show in SHOWS_CONFIG]
    show_options.append("🚀 RUN ALL (Batch Mode)")
    
    selected_show = st.selectbox(
        "Select Target:", 
        show_options, 
        index=None, 
        placeholder="Choose a show to process...",
        disabled=st.session_state.email_is_running
    )
    
    if selected_show:
        if st.button("📩 Trigger Email Pipeline", use_container_width=True, type="primary", disabled=st.session_state.email_is_running):
            st.session_state.email_is_running = True
            st.session_state.email_completed = False
            
            targets = SHOWS_CONFIG if selected_show == "🚀 RUN ALL (Batch Mode)" else [s for s in SHOWS_CONFIG if s["show_name"] == selected_show]
            
            for target_config in targets:
                show_title = target_config["show_name"]
                st.subheader(f"Processing: {show_title}")
                
                try:
                    with st.status(f"Pipeline running for {show_title}...", expanded=True) as status_box:
                        
                        # 1. SQL Fetch
                        status_box.write("⏳ Extracting legacy SQL Data...")
                        metrics = fetch_metrics(target_config)
                        status_box.write("✅ SQL Data Extracted Successfully.")
                        
                        # 2. Power BI PDF
                        status_box.write("⏳ Exporting PDF from Power BI Service (This will take a moment)...")
                        pdf_content = export_pbi_pdf(target_config['pbi_workspace_id'], target_config['pbi_report_id'], status_box)
                        status_box.write("✅ Power BI PDF Export Complete.")
                        
                        # 3. PDF to PNG Preview
                        status_box.write("⏳ Slicing PDF into PNG preview image...")
                        doc = fitz.open(stream=pdf_content, filetype="pdf")
                        pix = doc.load_page(0).get_pixmap(matrix=fitz.Matrix(2, 2))
                        png_bytes = pix.tobytes("png")
                        doc.close()
                        status_box.write("✅ PNG Preview Generated.")
                        
                        # 4. Email Build
                        status_box.write("⏳ Assembling HTML Email Template...")
                        html_body = build_email_body(target_config, metrics)
                        status_box.write("✅ Email Template Assembled.")
                        
                        # 5. Sending via Graph
                        status_box.write(f"⏳ Transmitting via Microsoft Graph API to {len(target_config['recipients'])} recipients...")
                        send_graph_email(target_config, html_body, pdf_content, png_bytes)
                        status_box.write("✅ Transmission Successful!")
                        
                        status_box.update(label=f"✅ {show_title} Completed Successfully!", state="complete")
                    
                    # Show Email Preview outside the status box for better visibility
                    with st.expander(f"👁️ View Sent Email Preview for {show_title}"):
                        st.info("Note: The preview image below is a placeholder in this web view, but the attached PDF is rendered correctly in the recipient's inbox.")
                        components.html(html_body, height=450, scrolling=True)
                        
                except Exception as e:
                    st.error(f"❌ Pipeline Failed for {show_title}: {str(e)}")
                    st.session_state.email_completed = True
                    st.stop()
            
            st.success("🎉 All selected pipelines finished successfully!")
            st.balloons()
            st.session_state.email_completed = True
            st.rerun()


# --- PAGE 3: DocID TOOL ---
elif page == "DocID Tool":
    st.title("🔍 DocID Tool")
    if st.button("🔄 Sync Live Database", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    df = fetch_doc_id_data()
    if not df.empty:
        st.markdown("### 📊 Database Summary")
        with st.container(border=True):
            s_col1, s_col2, s_col3 = st.columns(3)
            s_col1.metric("Total Distinct Shows", df['ShowId'].nunique())
            s_col2.metric("Total Distinct Theatres", df['TheatreId'].nunique())
            s_col3.metric("Total Document IDs", df['DocumentTypeId'].nunique())
        
        st.divider()
        col1, col2 = st.columns(2)
        shows = sorted(df['ShowName'].unique().tolist())
        with col1: sel_show = st.selectbox("Show Name:", ["All"] + shows)
        filtered_v_df = df if sel_show == "All" else df[df['ShowName'] == sel_show]
        theatre_list = sorted(filtered_v_df['TheatreName'].unique().tolist())
        with col2: sel_venue = st.selectbox("Theatre Name:", ["All"] + theatre_list)

        search_query = st.text_input("Fuzzy Search:")
        final_df = df.copy()
        if sel_show != "All": final_df = final_df[final_df['ShowName'] == sel_show]
        if sel_venue != "All": final_df = final_df[final_df['TheatreName'] == sel_venue]
        if search_query:
            clean_q = normalize_text(search_query)
            final_df = final_df[final_df['ShowName'].apply(lambda x: clean_q in normalize_text(x))]

        st.dataframe(final_df[['ShowName', 'TheatreName', 'DocumentName', 'ShowId', 'TheatreId', 'DocumentTypeId']].rename(columns={'ShowName': 'Show Name', 'TheatreName': 'Theatre Name', 'DocumentName': 'Document Name', 'ShowId': 'Show ID', 'TheatreId': 'Venue ID', 'DocumentTypeId': 'Document ID'}), use_container_width=True, hide_index=True)

st.sidebar.caption(f"v6.0 | Manual Status Clearance | {datetime.now().strftime('%H:%M:%S')}")