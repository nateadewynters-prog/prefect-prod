# 📧 Email Extraction Automation (Prefect 3.0)

**Host:** `DEW-DBSYNC01`
**Status:** 🟢 Active (Polling every 60s)
**Python:** 3.13.5
**Memory Constraint:** ⚠️ **CRITICAL: 760MB RAM Limit**

---

## 1. Project Overview

This project automates the extraction of financial data from incoming emails, replacing the manual Streamlit "Extraction Portal".

It operates as a **headless background service** that:

1. Polls a specific Outlook inbox via Microsoft Graph API.
2. Identifies relevant emails based on Sender and Subject.
3. Downloads attachments immediately to disk (Disk-First Processing).
4. Parses data using specific logic for **Malvern Theatres** (PDF) and **Sistic** (Excel).
5. Outputs clean CSVs and sends status notifications to MS Teams.

---

## 2. Architecture & Logic

### 🔄 Stateless Polling (Read-Only Workaround)

Due to security restrictions, the service has **Mail.Read** permissions but **cannot** mark emails as "Read" or move them to subfolders.

To prevent processing the same email twice, the flow maintains a local history file:

* **File:** `data/processed_ids.txt`
* **Logic:**
1. Fetch latest 50 emails.
2. Check if `Message-ID` exists in `processed_ids.txt`.
3. If **New**: Process -> Append ID to file.
4. If **Exists**: Skip.



### 🧠 Routing Logic

The flow routes emails to specific parsers based on the following rules:

| Route Name | Trigger Condition | Parser Module | Input Type |
| --- | --- | --- | --- |
| **MALVERN** | **From:** `figures@malvern-theatres.com`<br>

<br>**Subject:** "Contractual Report" | `malvern_theatre_parser.py` | PDF |
| **SISTIC** | **From:** `sisticadmin@sistic.com.sg`<br>

<br>**Subject:** "Settlement" | `sistic_agency_parser.py` | Excel (XLS/XLSX) |

### 💾 Memory Management (760MB RAM)

The server has extremely limited memory. To prevent `MemoryError`:

* **Disk-First:** Attachments are streamed directly to `data/inbox` and deleted from RAM variables immediately (`del content_bytes`).
* **Aggressive GC:** `gc.collect()` is invoked manually after every file operation.
* **Streamed Parsing:** Parsers read files from disk paths, never from in-memory byte buffers.

---

## 3. Directory Structure

C:\Prefect\outlook_automation\
├── email_extraction_flow.py    # Main Orchestrator (Prefect Flow)
├── outlook_utils.py            # Shared Utils (Teams, Env) [Renamed from brandwatch_utils]
├── .env                        # Credentials (API Keys)
├── parsers\
│   ├── __init__.py
│   ├── malvern_theatre_parser.py
│   └── sistic_agency_parser.py
└── data\
    ├── inbox\                  # Raw downloads (Temp)
    ├── processed\              # Final clean CSVs
    ├── archive\                # Successfully parsed raw files
    ├── failed\                 # Files that caused errors
    └── processed_ids.txt       # Idempotency history tracke

```

---

## 4. Parser Technical Details

### A. Malvern Theatres (`malvern_theatre_parser.py`)

* **Input:** "Contractual Report" PDFs.
* **Technique:** Regex Pattern Matching (Non-coordinate based).
* **Logic:** Scans for the pattern `Day -> Date -> Time -> Show Name -> Numbers`. It handles variable-length show titles by looking for gaps of 2+ spaces before the "Capacity" column.
* **Verification:** Compares the sum of extracted rows against the "Summary Totals" footer in the PDF.

### B. Sistic Agency (`sistic_agency_parser.py`)

* **Input:** "Event Settlement" Excel Grids.
* **Technique:** Anchor & Scan.
* **Logic:**
1. Scans rows until it finds **"Summary Totals"** (The Anchor).
2. Scans **Upwards** (approx. 5 rows) to find the Event Code.
3. Scans **Downwards** to find "Value Total" (Paid) and "Comp Total" (Free).


* **Verification:** Compares extracted totals against the "Event Span Total" footer.

---

## 5. Configuration & Setup

### Environment Variables (`.env`)

Create a `.env` file in the root directory:

```ini
# Azure AD / Graph API
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret
TARGET_EMAIL_USER=reports@dewynters.com

# MS Teams Webhook (Shared)
TEAMS_WEBHOOK_URL=https://dewyntersltd.webhook.office.com/...

```

### Dependencies

Ensure the virtual environment is active (`C:\Prefect\venv`):

```powershell
pip install msal pandas pdfplumber xlrd openpyxl prefect requests

```

---

## 6. Service Deployment (NSSM)

This flow runs as a persistent Windows Service.

**Service Name:** `outlook-extraction-service`

To register or update the service:

```powershell
nssm install outlook-extraction-service "C:\Prefect\venv\Scripts\python.exe" "C:\Prefect\outlook_automation\email_extraction_flow.py"
nssm set outlook-extraction-service AppDirectory "C:\Prefect\outlook_automation"
nssm set outlook-extraction-service AppStdout "C:\Prefect\logs\outlook_out.log"
nssm set outlook-extraction-service AppStderr "C:\Prefect\logs\outlook_err.log"

```

*Note: The script uses `flow.serve()` internally, so it manages its own polling schedule (Cron: `* * * * *`).*

---

## 7. Troubleshooting

| Issue | Cause | Fix |
| --- | --- | --- |
| **Flow crashes silently** | OOM (Out of Memory). | Check `gc.collect()` calls. Ensure no large objects (PDFs/Base64 strings) are held in variables. |
| **Re-processing old emails** | `processed_ids.txt` deleted or corrupt. | Restore from backup or let it re-process (CSVs will just overwrite with same data). |
| **Graph API 401 Unauthorized** | Expired Client Secret. | Update `AZURE_CLIENT_SECRET` in `.env` and restart service. |
| **"Access Denied" on File Move** | File still open in Python. | Ensure `with open(...)` blocks are closed before `shutil.move` is called. |