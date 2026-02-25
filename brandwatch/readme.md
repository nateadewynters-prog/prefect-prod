# 📊 Brandwatch API Sync (Prefect 3.0)

**Host:** `dew-insights01`  
**Outputs:** `/opt/data/brandwatch_outputs/`  
**Database:** `DEW-DBSYNC01` → `OrganicSocial`  

> **System Map Reference:** For global server settings, systemd service management, Git workflows, and Python environment details, refer to the **Master README** at `/opt/prefect/prod/code/readme.md`.

---

## 1. Project Overview

This project extracts social media performance data from the **Brandwatch (Falcon.io) API** and synchronizes it directly into the local SQL Server database.

It consists of **three distinct ETL pipelines**, orchestrated via Prefect:

- Channel-Level Metrics  
- Post-Level Metrics & Daily Delta Calculations  
- Comments / Replies / DMs Export  

All scripts share centralized credentials and utilities from:

- `/opt/prefect/prod/.env`  
- `/opt/prefect/prod/code/shared_libs/utils.py`  

---

## 2. Directory Structure & Dependencies

```text
/opt/prefect/prod/code/brandwatch/
├── readme.md                     <-- Project Documentation (This File)
├── brandwatch_channel_sync.py    # Fetches high-level page/channel metrics
├── brandwatch_content_sync.py    # Fetches post-level metrics & calculates daily deltas
└── brandwatch_comments_sync.py   # Asynchronously exports comment/reply data
```

---

## 3. Pipeline Logic

### A. Channel Sync (brandwatch_channel_sync.py)

**Schedule:** Daily @ 07:00 AM

**Logic:**

- Fetches T-2 (two days ago) data to account for API lag
- Pulls high-level page/channel metrics
- Writes results into `bwChannel`

**Idempotency Strategy:** Before inserting new rows, it deletes existing records in `bwChannel` for the specific target date only.

### B. Content Sync (brandwatch_content_sync.py)

**Schedule:** Daily @ 08:00 AM

**Logic:**

- Asynchronously fetches post-level metrics in 3 historical batches covering the last 90 days.
- Writes to `bwContent` and `bwContent_Reporting`.

**🔥 Key Engineering Features**

#### 1️⃣ Asynchronous Concurrency (httpx & asyncio)

Eliminates sequential blocking network calls. The script uses an `httpx.AsyncClient` and `asyncio.gather` to fetch posts and poll report statuses concurrently across multiple days, drastically reducing pipeline runtime.

#### 2️⃣ Dynamic Rate Limit Protection

Replaces legacy hardcoded sleep timers (the old 3-minute cooldowns). The script uses an `asyncio.Semaphore` to safely cap simultaneous connections and dynamically catches HTTP 429 Too Many Requests errors, pausing execution precisely based on the API's native `Retry-After` header.

#### 3️⃣ Vectorized Data Assembly & Delta Calculation

Replaces legacy SQL Stored Procedures and slow native Python loops.

- Post payloads and insight metrics are merged instantly using high-speed `pandas.merge()`.
- Daily growth metrics (deltas) are calculated in Pandas memory by comparing today's snapshot against historical values before pushing into `bwContent_Reporting`.

#### 4️⃣ Chunked SQL History Fetch

To prevent pyodbc silently failing on massive `IN (...)` clauses:

- Unique content_ids are broken into batches of 500
- Fetched in chunks
- Reassembled using `pd.concat()`

#### 5️⃣ Timezone Cleaning

Forces UTC parsing and removes timezone awareness to prevent datetime subtraction errors:

```python
df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce').dt.tz_localize(None)
```

#### 6️⃣ Data Contract Validation

Implements the `ValidationResult` dataclass to assess data loads.

If an API payload returns 0 rows during delta calculation, the system:

- Flags the run as `UNVALIDATED`
- Triggers a manual review alert to Teams
- Publishes results to the Prefect UI via Markdown Artifacts

This prevents silent data corruption.

### C. Comments Sync (brandwatch_comments_sync.py)

**Schedule:** Daily @ 09:30 AM

**Logic:**

- Initiates asynchronous export job
- Extracts Comments, Replies, and Direct Messages for T-2

**Export Polling Strategy:**

- Polls the Brandwatch Export API every 10 seconds
- Continues until job status = `COMPLETED`

---

## 4. Verification Checklist

### ✅ Prefect Dashboard

- Verify all three flows show `Completed` state
- Review the Artifacts tab for Data Contract Metrics
  - `PASSED`
  - `UNVALIDATED`
  - `FAILED`

### ✅ Output Files

Check:

```
/opt/data/brandwatch_outputs/
```

Confirm fresh CSVs are generated.

### ✅ Teams Notifications

Confirm receipt of Adaptive Cards in the MS Teams channel.

- `PASSED` validations are silenced
- `UNVALIDATED` (Warnings) and `FAILED` trigger alerts

### ✅ Database Validation

Confirm records exist for yesterday's reporting date in:

- `bwChannel`
- `bwContent_Reporting`

---

## 5. Data Engineering Troubleshooting

**Issue:** String data, right truncation

- **Cause:** pyodbc bulk insert buffer too small.
- **Fix:** Disable fast execution on the cursor:

```python
cursor.fast_executemany = False
```

**Issue:** TypeError: Cannot subtract tz-naive and tz-aware datetime-like objects

- **Cause:** Mixing timezone-aware and timezone-naive datetimes.
- **Fix:** Normalize both columns to timezone-naive:

```python
dt.tz_localize(None)
```

**Issue:** Daily metrics perfectly match lifetime metrics

- **Cause:** Historical SQL fetch failed due to oversized `IN (...)` clause.
- **Fix:** Chunk content_ids into smaller batches (e.g., 500).

**Issue:** SQL Connection Failures on Linux

- **Cause:** Missing or incorrect ODBC driver.
- **Fix:** Ensure `ODBC Driver 18 for SQL Server` is installed and referenced in connection strings.

```python
DRIVER={ODBC Driver 18 for SQL Server}
```

---

## ✅ Architectural Guarantees

This system is:

- **Idempotent**
- **Rate-limit aware**
- **SQL-safe**
- **Timezone-clean**
- **Delta-calculating in-memory**
- **Centrally credentialed**
- **Fully observable** via Prefect Artifacts

Production-hardened for high-volume social data ingestion.
