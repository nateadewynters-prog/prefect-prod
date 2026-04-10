"""
Main entry point for the Streamlit application.
This file handles navigation, session state initialization, and page routing.
"""

import streamlit as st
from datetime import datetime
from utils.auth import get_token
from views.dashboard_refresher import render_dashboard_refresher
from views.email_sender import render_email_sender
from views.docid_tool import render_docid_tool

# --- PAGE CONFIG ---
st.set_page_config(page_title="PBI & Data Tool", page_icon="📊", layout="wide")

# --- SESSION STATE INITIALIZATION ---
def init_session_state():
    # Page 1: Dashboard Refresher States
    if "is_refreshing" not in st.session_state: st.session_state.is_refreshing = False
    if "active_report" not in st.session_state: st.session_state.active_report = None
    if "refresh_complete" not in st.session_state: st.session_state.refresh_complete = False

    # Page 2: Email Sender States
    if "email_is_running" not in st.session_state: st.session_state.email_is_running = False
    if "email_completed" not in st.session_state: st.session_state.email_completed = False

init_session_state()

# --- HEADER COMPONENT ---
@st.fragment(run_every=10)
def connection_status_header():
    """
    Renders a live connection status indicator in the top right.
    """
    c1, c2 = st.columns([4, 1])
    with c2:
        token = get_token()
        color = "#10b981" if token else "#ef4444"
        label = "Connected 🟢" if token else "Disconnected 🔴"
        st.markdown(
            f"<div style='text-align: right;'>"
            f"<span style='color: {color}; font-weight: bold; font-size: 0.85rem;'>{label}</span><br>"
            f"<span style='color: #94a3b8; font-size: 0.7rem;'>Last Checked: {datetime.now().strftime('%H:%M:%S')}</span>"
            f"</div>", 
            unsafe_allow_html=True
        )

# --- MAIN APP EXECUTION ---
def main():
    connection_status_header()

    # Sidebar Navigation
    st.sidebar.title("🛠️ Tools")
    page = st.sidebar.radio(
        "Go to:", 
        ["Dashboard Refresher", "Automated Email Sender", "DocID Tool"]
    )

    # Routing
    if page == "Dashboard Refresher":
        render_dashboard_refresher()
    elif page == "Automated Email Sender":
        render_email_sender()
    elif page == "DocID Tool":
        render_docid_tool()

    # Sidebar Footer
    st.sidebar.caption(f"v6.0 | Modular Architecture | {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()
