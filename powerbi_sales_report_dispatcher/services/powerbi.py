"""
services/powerbi.py — Power BI REST calls, plus the PDF -> PNG preview.

Each function here is a single HTTP round trip. The *polling loops* stay in
pipeline.py on purpose: every poll emits a "⏳ Refresh Status: ..." line to the
live log, and a generator is the only thing that can yield those out to the
browser. Keeping the loop there means the UI progress behaviour is unchanged.
"""

import fitz
import requests


def _dataset_base(workspace_id, dataset_id):
    return f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}"


def _report_base(workspace_id, report_id):
    return f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}"


def auth_headers(pbi_token):
    return {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# DATASET REFRESH
# ---------------------------------------------------------------------------
def trigger_refresh(pbi_headers, workspace_id, dataset_id):
    """Returns the raw response, un-raised, so the caller can tell a
    'refresh already executing' 400 apart from a real error (see pipeline.py)."""
    return requests.post(f"{_dataset_base(workspace_id, dataset_id)}/refreshes", headers=pbi_headers, json={})


def get_refresh_status(pbi_headers, workspace_id, dataset_id):
    poll_req = requests.get(f"{_dataset_base(workspace_id, dataset_id)}/refreshes?$top=1", headers=pbi_headers)
    poll_req.raise_for_status()
    return poll_req.json().get('value', [{}])[0].get('status', 'Unknown')


# ---------------------------------------------------------------------------
# REPORT EXPORT
# ---------------------------------------------------------------------------
def start_export(pbi_headers, workspace_id, report_id, fmt="PDF"):
    resp = requests.post(f"{_report_base(workspace_id, report_id)}/ExportTo", headers=pbi_headers, json={"format": fmt})
    resp.raise_for_status()
    return resp.json().get("id")


def get_export_status(pbi_headers, workspace_id, report_id, export_id):
    poll_req = requests.get(f"{_report_base(workspace_id, report_id)}/exports/{export_id}", headers=pbi_headers)
    poll_req.raise_for_status()
    return poll_req.json().get("status")


def download_export(pbi_headers, workspace_id, report_id, export_id):
    return requests.get(f"{_report_base(workspace_id, report_id)}/exports/{export_id}/file", headers=pbi_headers).content


# ---------------------------------------------------------------------------
# PREVIEW IMAGE
# ---------------------------------------------------------------------------
def pdf_first_page_to_png(pdf_bytes):
    doc = fitz.open("pdf", pdf_bytes)
    pix = doc.load_page(0).get_pixmap(dpi=150)
    png_bytes = pix.tobytes("png")
    doc.close()
    return png_bytes
