"""
services/powerbi.py — Power BI report export and dataset refresh.

Two jobs use this module:
  - a dispatch exports the report (PPTX for the attachment, PDF→PNG for the
    inline preview) via export_report + pdf_first_page_to_png;
  - a refresh finds the report's dataset and triggers a data refresh via
    find_dataset_id + trigger_refresh / poll_refresh_status.

Both the export and the refresh are asynchronous on Power BI's side: you
kick them off, then poll a status URL until it says done. The callers in
pipeline.py drive that polling so they can stream progress to the UI.
"""

import time

import requests
import fitz  # PyMuPDF, for turning the PDF export into a PNG preview


def _report_base(workspace_id: str, report_id: str) -> str:
    return f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}"


def _workspace_base(workspace_id: str) -> str:
    return f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"


def powerbi_export(pbi_token: str, workspace_id: str, report_id: str,
                   fmt: str, log=None) -> bytes:
    """Trigger a Power BI 'ExportTo', poll until done, return the raw file bytes.

    fmt is 'PPTX' or 'PDF'. Raises on failure.
    """
    headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
    base = _report_base(workspace_id, report_id)

    resp = requests.post(f"{base}/ExportTo", headers=headers, json={"format": fmt})
    resp.raise_for_status()
    export_id = resp.json()["id"]

    status_url = f"{base}/exports/{export_id}"
    while True:
        time.sleep(5)
        status = requests.get(status_url, headers=headers).json().get("status")
        if log:
            log(f"⏳ {fmt} export: {status}...")
        if status == "Succeeded":
            break
        if status == "Failed":
            raise RuntimeError(f"Power BI {fmt} export failed")

    return requests.get(f"{status_url}/file", headers=headers).content


def pdf_first_page_to_png(pdf_bytes: bytes) -> bytes:
    """Render page 1 of the exported PDF to a PNG (used as the inline preview)."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pix = doc.load_page(0).get_pixmap(matrix=fitz.Matrix(2, 2))
    png_bytes = pix.tobytes("png")
    doc.close()
    return png_bytes


# ---------------------------------------------------------------------------
# DATASET REFRESH
# The dataset ID isn't stored in SHOWS_CONFIG — we look it up from the report,
# since every report knows the dataset it's built on.
# ---------------------------------------------------------------------------
def find_dataset_id(pbi_token: str, workspace_id: str, report_id: str) -> str | None:
    """Return the dataset a report is built on, or None if it has none."""
    headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
    report = requests.get(f"{_report_base(workspace_id, report_id)}", headers=headers)
    report.raise_for_status()
    return report.json().get("datasetId")


def trigger_refresh(pbi_token: str, workspace_id: str, dataset_id: str):
    """Kick off a dataset refresh. Returns the raw response so the caller can
    tell 'already running' apart from a real error (see pipeline.py)."""
    headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
    url = f"{_workspace_base(workspace_id)}/datasets/{dataset_id}/refreshes"
    return requests.post(url, headers=headers, json={})


def poll_refresh_status(pbi_token: str, workspace_id: str, dataset_id: str) -> str:
    """Return the status of the most recent refresh ('Completed'/'Failed'/...)."""
    headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
    url = f"{_workspace_base(workspace_id)}/datasets/{dataset_id}/refreshes?$top=1"
    poll = requests.get(url, headers=headers)
    poll.raise_for_status()
    return poll.json().get("value", [{}])[0].get("status", "Unknown")
