"""
Utility functions for interacting with the Power BI REST API.
"""

import httpx
import requests
import json
import os
import time
import pandas as pd
import streamlit as st
from config import JSON_PATH
from utils.auth import get_token

@st.cache_data
def load_pbi_report_data():
    """
    Loads Power BI report mapping details from a local JSON configuration.
    """
    if not os.path.exists(JSON_PATH):
        return pd.DataFrame()
    with open(JSON_PATH, 'r') as f:
        df = pd.DataFrame(json.load(f))
    return df.dropna(subset=['Underlying Dataset ID']).sort_values(by='Report Name')

async def trigger_pbi_refresh(workspace_id, dataset_id, headers):
    """
    Asynchronously triggers a Power BI dataset refresh.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    async with httpx.AsyncClient(timeout=30.0) as client:
        return await client.post(url, headers=headers, json={})

async def check_pbi_status(workspace_id, dataset_id, headers):
    """
    Asynchronously retrieves the latest refresh status for a dataset.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)
        return resp.json().get('value', [{}])[0] if resp.status_code == 200 else {}

def export_pbi_pdf(workspace_id, report_id, status_container):
    """
    Exports a Power BI report to a PDF file using the ExportTo API.
    """
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
