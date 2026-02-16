# Project State: Brandwatch to Prefect 3.0 Migration
**Date:** February 13, 2026
**Status:** Operational (Services Running, Dashboard Accessible, Teams Notifications Enabled)

## 1. System & Environment Context
* **Hostname:** DEW-DBSYNC01 (Internal IP: 10.1.50.126)
* **OS:** Windows Server 10.0.14393
* **Python:** 3.13.5 (Located at `C:\Program Files\Python313`)
* **Project Root:** `C:\Prefect\brandwatch`
* **Virtual Env:** `C:\Prefect\venv`
* **Output Directory:** `C:\BrandwatchOutputs\`
* **Service Manager:** NSSM (Path: `C:\Users\batchuser\nssm-2.24-101-g897c7ad\win64\nssm.exe`)

> **⚠️ Important Note for PowerShell:**
> All NSSM commands must use the specific executable path syntax:
> `& "C:\Users\batchuser\nssm-2.24-101-g897c7ad\win64\nssm.exe" <command>`

## 2. Architecture Overview

The project migrates three legacy scripts into a unified Prefect 3.0 orchestration layer. To maintain code quality and reduce redundancy (DRY principles), common infrastructure logic has been extracted into a shared utility module.

### Core Components
* **Prefect Server:** Runs locally as a Windows Service, exposed to the network on port 4200.
* **Shared Utilities (`brandwatch_utils.py`):** Centralizes SQL connections, Environment loading, and Teams notifications.
* **Channel Flow:** Fetches channel-level metrics daily at 07:00 AM.
* **Content Flow:** Fetches post-level metrics in 3 batches (Last 90 days) daily at 08:00 AM.
* **Comments Flow:** Fetches comments, replies, and DMs daily at 09:30 AM.

## 3. Service Configuration (NSSM)
* **Service 1: prefect-server**
    * Command: `C:\Prefect\venv\Scripts\python.exe`
    * Arguments: `-m prefect server start --host 0.0.0.0`
    * Env Vars: `PREFECT_UI_API_URL=http://10.1.50.126:4200/api`
    * Purpose: Hosts the UI and API, accessible remotely via `http://10.1.50.126:4200`.

* **Service 2: brandwatch-channel-service**
    * Command: `C:\Prefect\venv\Scripts\python.exe`
    * Arguments: `brandwatch_channel_sync.py`
    * Directory: `C:\Prefect\brandwatch`
    * Schedule: Daily @ 07:00 AM.

* **Service 3: brandwatch-content-service**
    * Command: `C:\Prefect\venv\Scripts\python.exe`
    * Arguments: `brandwatch_content_sync.py`
    * Directory: `C:\Prefect\brandwatch`
    * Schedule: Daily @ 08:00 AM.

* **Service 4: brandwatch-comments-service**
    * Command: `C:\Prefect\venv\Scripts\python.exe`
    * Arguments: `brandwatch_comments_sync.py`
    * Directory: `C:\Prefect\brandwatch`
    * Schedule: Daily @ 09:30 AM.

## 4. Codebase Summary

### A. Shared Utilities (`brandwatch_utils.py`)
* **Purpose:** Eliminates code duplication across the three flows.
* **Functions:**
    * `setup_environment()`: Loads `.env`.
    * `get_db_connection()`: Returns a ready-to-use `pyodbc` connection object.
    * `send_teams_notification()`: Standardized Success/Failure cards for MS Teams.

### B. Channel Sync Script (`brandwatch_channel_sync.py`)
* **Logic:** Fetches "Yesterday's" data (2 days ago).
* **Key Features:** Retries API fetch 3 times; explicit check for empty DataFrames before SQL push. Uses `utils` for SQL and Alerts.
* **Outputs:** `C:\BrandwatchOutputs\channel\`

### C. Content Sync Script (`brandwatch_content_sync.py`)
* **Logic:** Fetches content metrics in 3 batches (Last 90 days).
* **Key Features:**
    * **Idempotent File Generation:** Safely checks for, deletes, and replaces existing daily CSVs.
    * **Python Delta Calculation:** Replaces legacy SQL Stored Procedure. Calculates daily snapshot-to-delta metrics directly in Pandas memory before pushing.
    * **Chunked SQL History Fetch:** Batches the historical SQL fetch into chunks of 500 `content_id`s to bypass pyodbc character string limits.
    * **Timezone & Data Cleaning:** Forces timezone-naive datetimes and strips whitespace.
    * **Rate Limits:** Sleeps 7 minutes between batches.
* **Outputs:** `C:\BrandwatchOutputs\content\`

### D. Comments Sync Script (`brandwatch_comments_sync.py`)
* **Logic:** Initiates an asynchronous export Job for comments/replies/DMs (2 days ago).
* **Key Features:**
    * **Polling:** Polls the API until the CSV export is `COMPLETED`.
    * **Idempotency:** Deletes existing SQL records for the target date range before inserting to prevent duplicates.
* **Outputs:** `C:\BrandwatchOutputs\comment\`

## 5. Version Control (Git)
The project is tracked using **Portable Git** (Local Repository).

* **Git Executable:** `C:\Users\batchuser\PortableGit\bin\git.exe`
* **Repository Root:** `C:\Prefect`
* **Ignored Items (`.gitignore`):**
    * `venv/` (Virtual Environment)
    * `.env` (API Keys & Secrets)
    * `BrandwatchOutputs/` (Data Files)
    * `__pycache__/`
* **Workflow:**
    1. Make changes in VS Code.
    2. Open PowerShell in `C:\Prefect`.
    3. $git = "C:\Users\batchuser\PortableGit\bin\git.exe"
    4. Run: `& "C:\Users\batchuser\PortableGit\bin\git.exe" add .`
    5. Run: `& "C:\Users\batchuser\PortableGit\bin\git.exe" commit -m "Message"`

## 6. Verification Checklist
1.  **Dashboard:** Open `http://10.1.50.126:4200`. Ensure 3 Deployments exist.
2.  **Logs:** Run a "Quick Run" for each flow and verify logs show successful database row insertions.
3.  **Files:** Check `C:\BrandwatchOutputs\` subdirectories for freshly generated CSVs.
4.  **Teams:** Verify a Success or Failure card is received in the relevant Teams channel for all three flows.

## 7. Troubleshooting & Common Issues

### Mandatory Service Restarts
> **⚠️ Critical:** Any changes made to the Python scripts (`.py`), the environment file (`.env`), or Prefect deployment configurations **require a manual restart of the Windows Services**. The services load the code into memory at startup; they will not reflect saved changes until restarted.

**Restart all Brandwatch Sync services:**
```powershell
Restart-Service -Name "brandwatch-*-service" -Verbose

### Issue: Remote Dashboard Access (Port 4200)
* **Cause:** Windows Firewall blocking inbound traffic to the Prefect UI.
* **Fix:** Added Inbound Rule for TCP Port 4200.
    ```powershell
    New-NetFirewallRule -DisplayName "Allow Prefect UI" -Direction Inbound -LocalPort 4200 -Protocol TCP -Action Allow
    ```

### Issue: Service is "Running" but Flow not showing in Dashboard
* **Cause:** The service cannot find the Prefect API.
* **Fix:** Ensure the `PREFECT_API_URL` environment variable is set for that specific service.
    ```powershell
    & "...\nssm.exe" set <ServiceName> AppEnvironmentExtra "PREFECT_API_URL=[http://10.1.50.126:4200/api](http://10.1.50.126:4200/api)"
    Restart-Service <ServiceName>
    ```

### Issue: `ValueError: '...' is not in the subpath of 'C:\Windows\system32'`
* **Cause:** The service is trying to run the script from the default System32 directory instead of the project folder. Prefect cannot calculate relative paths from System32.
* **Fix:** Set the `AppDirectory` to the project root.
    ```powershell
    & "...\nssm.exe" set <ServiceName> AppDirectory "C:\Prefect\brandwatch"
    Restart-Service <ServiceName>
    ```

### Issue: `String data, right truncation: length 2038 buffer 510`
* **Cause:** `pyodbc`'s `fast_executemany` feature attempts to guess a memory buffer size (usually ~255 chars). It crashes when attempting to bulk insert long strings like social media captions or image URLs.
* **Fix:** Disable `fast_executemany` on the database cursor before executing the insert.
    ```python
    cursor = conn.cursor()
    # cursor.fast_executemany = True  <-- Remove or comment out this line
    ```

### Issue: `TypeError: Cannot subtract tz-naive and tz-aware datetime-like objects`
* **Cause:** Attempting to do math in Pandas between an API-provided date (which has a UTC timezone) and a locally generated date (which lacks a timezone).
* **Fix:** Force both columns to be parsed with UTC and then strip the timezone entirely before doing math.
    ```python
    df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce').dt.tz_localize(None)
    ```

### Issue: Daily metrics perfectly match lifetime metrics (Deltas not calculating)
* **Cause:** Pandas failed to fetch the previous historical records because the SQL `IN (...)` clause was too large (over 100,000 characters for 4,000+ IDs). `pyodbc` silently dropped the filter, returning 0 historical records.
* **Fix:** Chunk the unique IDs into smaller batches (e.g., 500) and iterate the SQL fetch, using `pd.concat()` to rebuild the history dataframe.