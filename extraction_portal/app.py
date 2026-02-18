import streamlit as st
import pandas as pd
import os
import gc
import pdfplumber
import xlrd
from parsers import pdf_contractual, xls_settlement
from datetime import timezone, timedelta
from dateutil import parser as date_parser

# --- CONFIGURATION ---
# We use a hardcoded path to ensure we don't accidentally fill up the wrong drive.
TEMP_DIR = r"C:\Prefect\extraction_portal\temp"
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

st.set_page_config(
    page_title="Data Extraction Portal",
    page_icon="📂",
    layout="wide"
)

# --- SIDEBAR INSTRUCTIONS ---
with st.sidebar:
    st.header("📖 Instructions")
    st.warning("⚠️ RAM LIMIT WARNING: Please process one file at a time.")
    st.markdown("""
    **Supported Files:**
    1. **Contractual Reports (PDF)**
       * Look for *'Contractual Report'* in the header.
       * *Logic:* Scans for show times and ticket counts.
    2. **Event Settlement (Excel)**
       * Messy grid layouts with *'Summary Totals'*.
       * *Logic:* Finds blocks of data and sums them up.
    
    **How to use:**
    1. Upload a file.
    2. Read the "Processing Logs" to verify accuracy.
    3. Download the Clean CSV.
    """)
    st.divider()
    st.caption(f"Temp Folder: `{TEMP_DIR}`")

# --- HELPER FUNCTIONS ---
def detect_file_type(filepath):
    """
    Returns 'pdf', 'excel', or None based on content keywords.
    Reads only the first few bytes/lines to save memory.
    """
    ext = os.path.splitext(filepath)[1].lower()
    
    if ext == '.pdf':
        try:
            with pdfplumber.open(filepath) as pdf:
                # Check first page for keywords
                if len(pdf.pages) > 0:
                    text = pdf.pages[0].extract_text()
                    if "Contractual Report" in text or "Performances from" in text:
                        return 'pdf'
        except:
            return None
            
    elif ext in ['.xls', '.xlsx']:
        try:
            # Read just the first 20 rows to avoid loading huge files
            engine = 'xlrd' if ext == '.xls' else 'openpyxl'
            df = pd.read_excel(filepath, header=None, nrows=20, engine=engine)
            
            # Convert to string to search for keywords like 'Event Settlement'
            content = df.to_string()
            if "Event Settlement" in content or "Event Code" in content:
                return 'excel'
        except:
            return None
            
    return None

# --- MAIN UI ---
st.title("📄 Manual Report Extractor")
st.markdown("""
Upload raw **PDF** or **Excel** reports here. 
The system will automatically detect the format, extract the data, and double-check the totals.
""")

uploaded_file = st.file_uploader("Upload Report", type=['pdf', 'xls', 'xlsx'])

if uploaded_file:
    # 1. DISK-FIRST SAVE (Critical for saving RAM)
    # We save the file to disk immediately instead of holding it in RAM.
    temp_path = os.path.join(TEMP_DIR, uploaded_file.name)
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    st.toast(f"File saved to temp: {uploaded_file.name}")

    try:
        # 2. ROUTER LOGIC
        file_type = detect_file_type(temp_path)
        
        data = []
        logs = []
        parser_name = ""

        if file_type == 'pdf':
            parser_name = "PDF Contractual Parser"
            st.info(f"🤖 Detected: {parser_name}")
            data, logs = pdf_contractual.extract_contractual_report(temp_path)
            
        elif file_type == 'excel':
            parser_name = "Excel Settlement Parser"
            st.info(f"🤖 Detected: {parser_name}")
            data, logs = xls_settlement.extract_settlement_data(temp_path)
            
        else:
            st.error("❌ Unknown file format.")
            st.markdown("""
            **I could not identify this file.** I looked for specific keywords like "Contractual Report" (PDF) or "Event Settlement" (Excel) but didn't find them.
            """)
            st.stop()

        # 3. DISPLAY LOGS (The "Story" of what happened)
        # We put this inside an expander so it doesn't clutter the screen unless requested.
        with st.expander("📜 Processing Logs (Click to verify accuracy)", expanded=True):
            for log in logs:
                if "❌" in log:
                    st.error(log)
                elif "✅" in log:
                    st.success(log)
                elif "⚠️" in log:
                    st.warning(log)
                else:
                    st.text(log)

        # 4. DISPLAY RESULTS
        if data:
            df_result = pd.DataFrame(data)
            
            st.divider()
            col1, col2 = st.columns([1, 3])
            with col1:
                st.metric("Rows Extracted", len(df_result))
            with col2:
                st.success("Extraction Complete! Please download your file below.")

            # Data Preview
            st.subheader("Data Preview")
            st.dataframe(df_result, height=300, use_container_width=True)

            # CSV Download
            csv = df_result.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Clean CSV",
                data=csv,
                file_name=f"Clean_{uploaded_file.name}.csv",
                mime="text/csv",
                type="primary"
            )
        else:
            st.warning("No data extracted. Please check the Verification Logs above to see why.")

    except Exception as e:
        st.error(f"Critical Error: {str(e)}")

    finally:
        # 5. CLEANUP (Critical for 760MB RAM)
        # We delete the temp file and force the Python Garbage Collector to run.
        if os.path.exists(temp_path):
            os.remove(temp_path)
        gc.collect() 
        print(f"[System] Cleaned up {temp_path} and ran gc.collect()")

else:
    # Idle Cleanup: If no file is uploaded, ensure temp dir is empty
    # This prevents junk from piling up if a previous session crashed.
    if os.path.exists(TEMP_DIR):
        for f in os.listdir(TEMP_DIR):
            try:
                os.remove(os.path.join(TEMP_DIR, f))
            except:
                pass