# 📊 Brandwatch API Sync (Prefect 3.0)

**Host:** `DEW-DBSYNC01`  
**Outputs:** `C:\BrandwatchOutputs\`  
**Database:** `DEW-DBSYNC01` → `OrganicSocial`  

> **System Map Reference:** For global server settings, NSSM service management, Git workflows, and Python environment details, refer to the **Master README** at `C:\Prefect\README.md`.

---

## 1. Project Overview

This project extracts social media performance data from the **Brandwatch (Falcon.io) API** and synchronizes it directly into the local SQL Server database.

It consists of **three distinct ETL pipelines**, orchestrated via Prefect:

- Channel-Level Metrics
- Post-Level Metrics & Daily Delta Calculations
- Comments / Replies / DMs Export

All scripts share centralized credentials and utilities from:

```
C:\Prefect\.env
C:\Prefect\shared_lib\utils.py
```

---

## 2. Directory Structure & Dependencies

```text
C:\Prefect\brandwatch\
├── README.md                     <-- Project Documentation (This File)
├── brandwatch_channel_sync.py    # Fetches high-level page/channel metrics
├── brandwatch_content_sync.py    # Fetches post-level metrics & calculates daily deltas
└── brandwatch_comments_sync.py   # Asynchronously exports comment/reply data
```

### 🔗 External Dependencies

```
C:\Prefect\.env                  # Contains BRANDWATCH_API_KEY and SQL Credentials
C:\Prefect\shared_lib\utils.py   # Unified DB connections and Teams Webhooks
```

> The scripts dynamically import the shared utility library by appending the parent directory `C:\Prefect` to the Python system path at runtime.

---

## 3. Pipeline Logic

---

### A. Channel Sync (`brandwatch_channel_sync.py`)

**Schedule:** Daily @ 07:00 AM  

**Logic:**

- Fetches **T-2 (two days ago)** data to account for API lag.
- Pulls high-level page/channel metrics.
- Writes results into `bwChannel`.

**Idempotency Strategy:**

Before inserting new rows:

- Deletes existing records in `bwChannel`
- For the specific target date only

This ensures retries never create duplicates.

---

### B. Content Sync (`brandwatch_content_sync.py`)

**Schedule:** Daily @ 08:00 AM  

**Logic:**

- Fetches post-level metrics in **3 historical batches**
- Covers the last **90 days**
- Writes to:
  - `bwContent`
  - `bwContent_Reporting`

---

#### 🔥 Key Engineering Features

##### 1️⃣ In-Memory Delta Calculation

Replaces the legacy SQL Stored Procedure.

Daily metrics such as:

- `daily_likes`
- `daily_views`
- `daily_comments`
- `daily_shares`

Are calculated in **Pandas memory** by comparing snapshot-to-snapshot values before pushing into `bwContent_Reporting`.

---

##### 2️⃣ Chunked SQL History Fetch

Problem:
- SQL `IN (...)` clauses exceeding ~100k characters
- pyodbc silently failing
- Returning 0 historical rows

Solution:
- Break unique `content_id`s into batches of **500**
- Fetch in chunks
- Reassemble using:

```python
pd.concat()
```

If post volume increases significantly, reduce chunk size.

---

##### 3️⃣ Timezone Cleaning

Ensures SQL-safe ingestion:

```python
df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce').dt.tz_localize(None)
```

This:

- Forces UTC parsing
- Removes timezone awareness
- Prevents datetime subtraction errors

---

##### 4️⃣ Rate Limit Protection

Between 30-day API batches:

- Enforces a **3-minute cooldown**
- Prevents Brandwatch throttling
- Protects long-running historical syncs

---

### C. Comments Sync (`brandwatch_comments_sync.py`)

**Schedule:** Daily @ 09:30 AM  

**Logic:**

- Initiates asynchronous export job
- Extracts:
  - Comments
  - Replies
  - Direct Messages
- Pulls T-2 data

---

#### 🔄 Export Polling Strategy

- Polls the Brandwatch Export API every **10 seconds**
- Waits until job status = `COMPLETED`
- Downloads generated CSV
- Loads into SQL (`bwComment`)

---

#### 🛡 Idempotency

Before insert:

- Deletes existing SQL records
- For the extracted date range only

Ensures safe re-runs.

---

## 4. Verification Checklist

### ✅ Prefect Dashboard

Open:

```
http://10.1.50.126:4200
```

Verify all three flows show **Completed** state.

---

### ✅ Output Files

Check:

```
C:\BrandwatchOutputs\
```

Subdirectories:

- `channel`
- `content`
- `comment`

Fresh CSVs should exist with today's timestamp.

---

### ✅ Teams Notifications

Confirm receipt of:

```
✅ Success
```

Adaptive Card in the MS Teams channel for all three flows.

---

### ✅ Database Validation

Run SQL checks:

- `bwContent_Reporting`  
  - Confirm `daily_*` columns are populated (not all 0)

- `bwChannel`  
  - Confirm records exist for yesterday’s reporting date

---

## 5. Data Engineering Troubleshooting

---

### ❌ Issue:  
`String data, right truncation: length 2038 buffer 510`

**Cause:**

`pyodbc`'s `fast_executemany` attempts to auto-size buffers (~255 chars).  
Fails when inserting long captions, deep links, or image URLs.

**Fix:**

Disable fast execution on the cursor:

```python
cursor.fast_executemany = False
```

This is intentionally disabled in the Content pipeline.

---

### ❌ Issue:  
`TypeError: Cannot subtract tz-naive and tz-aware datetime-like objects`

**Cause:**

Mixing:

- API-provided UTC timestamps (tz-aware)
- Locally generated naive datetimes

**Fix:**

Normalize both to timezone-naive:

```python
df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce').dt.tz_localize(None)
```

---

### ❌ Issue:  
Daily metrics perfectly match lifetime metrics (deltas not calculating)

**Cause:**

SQL `IN (...)` clause too large.  
pyodbc silently drops filter → returns 0 history → script assumes all posts are new.

**Fix:**

Chunk `content_id`s into smaller batches (e.g., 500):

```python
for chunk in chunks:
    # fetch history
```

Rebuild full historical dataframe using:

```python
pd.concat()
```

Reduce chunk size further if post volume increases.

---

## ✅ Architectural Guarantees

This system is:

- Idempotent
- Rate-limit aware
- SQL-safe
- Timezone-clean
- Delta-calculating in-memory
- Centrally credentialed
- Fully observable via Prefect
- Production-hardened for high-volume social data ingestion

---