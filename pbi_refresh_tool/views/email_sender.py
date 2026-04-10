"""
UI logic for the Automated Email Sender page.
"""

import streamlit as st
import streamlit.components.v1 as components
import fitz  # PyMuPDF
from config import SHOWS_CONFIG
from utils.database import fetch_metrics
from utils.powerbi import export_pbi_pdf
from utils.email_service import build_email_body, send_graph_email

def render_email_sender():
    """
    Renders the Power BI Automated Email Send interface.
    """
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
