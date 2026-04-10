"""
UI logic for the DocID Tool page.
"""

import streamlit as st
import re
from utils.database import fetch_doc_id_data

def normalize_text(text):
    """
    Normalizes text for fuzzy searching by removing non-alphanumeric characters and lowercasing.
    """
    if not text:
        return ""
    return re.sub(r'[^a-zA-Z0-9]', '', str(text)).lower()

def render_docid_tool():
    """
    Renders the DocID Tool search interface.
    """
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
        with col1:
            sel_show = st.selectbox("Show Name:", ["All"] + shows)
        
        filtered_v_df = df if sel_show == "All" else df[df['ShowName'] == sel_show]
        theatre_list = sorted(filtered_v_df['TheatreName'].unique().tolist())
        
        with col2:
            sel_venue = st.selectbox("Theatre Name:", ["All"] + theatre_list)

        search_query = st.text_input("Fuzzy Search:")
        final_df = df.copy()
        
        if sel_show != "All":
            final_df = final_df[final_df['ShowName'] == sel_show]
        if sel_venue != "All":
            final_df = final_df[final_df['TheatreName'] == sel_venue]
        
        if search_query:
            clean_q = normalize_text(search_query)
            final_df = final_df[final_df['ShowName'].apply(lambda x: clean_q in normalize_text(x))]

        st.dataframe(
            final_df[['ShowName', 'TheatreName', 'DocumentName', 'ShowId', 'TheatreId', 'DocumentTypeId']].rename(
                columns={
                    'ShowName': 'Show Name',
                    'TheatreName': 'Theatre Name',
                    'DocumentName': 'Document Name',
                    'ShowId': 'Show ID',
                    'TheatreId': 'Venue ID',
                    'DocumentTypeId': 'Document ID'
                }
            ), 
            use_container_width=True, 
            hide_index=True
        )
