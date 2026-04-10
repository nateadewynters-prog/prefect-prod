"""
UI logic for the Dashboard Refresher page.
"""

import streamlit as st
import asyncio
import time
from datetime import datetime
from utils.auth import get_token
from utils.powerbi import load_pbi_report_data, trigger_pbi_refresh, check_pbi_status

def render_dashboard_refresher():
    """
    Renders the Power BI Refresh Center interface.
    """
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
                    # Note: streamlit doesn't support running async functions directly in the UI loop easily
                    # so we use asyncio.run for these small calls.
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
                                time.sleep(8)
                    else:
                        st.error(f"API Error {response.status_code}")
                        st.session_state.refresh_complete = True
            else:
                st.info("Refresh process finished. Review logs above and click 'Clear Status' to continue.")
