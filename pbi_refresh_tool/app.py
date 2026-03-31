import streamlit as st
import json
import msal
import httpx
import asyncio
import os
import re
import pyodbc
import pandas as pd
from datetime import datetime
import time 
from dotenv import load_dotenv

# --- CONFIG & DATA ---
load_dotenv()
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
JSON_PATH = './data/PowerBI_Report_IDs.json'

# Database Configuration
DB_CONFIG = {
    'server': 'dewyntersticketsql.database.windows.net',
    'database': 'TicketingDS',
    'username': 'insightlogin@dewyntersticketsql.database.windows.net',
    'password': 'Fd2CxqEfxVxy7y',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

st.set_page_config(page_title="PBI & Data Tool", page_icon="📊", layout="wide")

# Initialize Session States
if "is_refreshing" not in st.session_state:
    st.session_state.is_refreshing = False
if "active_report" not in st.session_state:
    st.session_state.active_report = None
if "refresh_complete" not in st.session_state:
    st.session_state.refresh_complete = False

# --- HELPERS ---
def normalize_text(text):
    if not text: return ""
    return re.sub(r'[^a-zA-Z0-9]', '', str(text)).lower()

def get_token():
    try:
        authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        app = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
        result = app.acquire_token_for_client(scopes=["https://analysis.windows.net/powerbi/api/.default"])
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
page = st.sidebar.radio("Go to:", ["Dashboard Refresher", "DocID Tool"])

# --- PAGE 1: DASHBOARD REFRESHER ---
if page == "Dashboard Refresher":
    st.title("📊 Power BI Refresh Center")
    
    # Reset Logic for the "Clear Status" button
    if st.session_state.refresh_complete:
        if st.button("🧹 Clear Status & Continue", use_container_width=True):
            st.session_state.is_refreshing = False
            st.session_state.active_report = None
            st.session_state.refresh_complete = False
            st.rerun()

    # Global Restriction Banner
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

    # --- MONITORING ENGINE ---
    if st.session_state.is_refreshing and st.session_state.active_report:
        st.divider()
        st.subheader(f"Monitoring: {st.session_state.active_report}")
        
        token = get_token()
        if token:
            headers = {"Authorization": f"Bearer {token}"}
            start_time = time.time()
            p_bar = st.progress(0, text="Initializing API handshake...")
            log_container = st.expander("Live Refresh Log", expanded=True)
            
            # We only run the async loop if we aren't already "complete"
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
                                st.rerun() # Refresh once to show the "Clear Status" button
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
                # Persistent view once finished
                st.info("Refresh process finished. Review logs above and click 'Clear Status' to continue.")

# --- PAGE 2: DocID TOOL ---
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

st.caption(f"v5.0 | Manual Status Clearance | {datetime.now().strftime('%H:%M:%S')}")