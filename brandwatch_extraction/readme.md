# 📊 Brandwatch Social Performance Extraction (Prefect 3.0)

**Host:** `dew-insights01`  
**Status:** 🟢 Active (Scheduled 8:30 AM Daily)  
**Orchestration:** Prefect 3.0  

---

## 1. Project Overview

This service is a high-performance ELT pipeline designed to extract social performance data from the Brandwatch (Falcon.io) API. It enforces an enterprise-grade **stateless container architecture**, moving data directly from the API to Azure SQL with zero local disk dependency.

---

## 2. Key Architectural Pillars

### 🧊 Stateless Container Design
This pipeline is strictly **pure API-to-Database**. There is no Docker volume mapping for data storage. 
- **In-Memory Streaming:** Engage comments are requested as exports and **streamed** (via `requests.get(stream=True)`) to avoid RAM exhaustion and local disk usage. Data is processed via a line-generator and inserted in batches of **500 rows**.
- **Direct Commits:** Data is committed to SQL as it is processed, ensuring the container can be destroyed and recreated on any host without data loss.

### 🎯 Granular Prefect Task Routing
The logic is decomposed into distinct Prefect `@tasks` to ensure high resilience and efficient retries.
- **Task-Level Retries:** If a transient API or SQL error occurs, Prefect retries **only that specific task** (e.g., `stage_data` has 3 retries), preventing full pipeline restarts. Note that Insight payloads are staged from inside `poll_insight` via a direct `insert_raw_json` call, bypassing `stage_data` — a SQL failure there is not retried in isolation and instead bubbles up to the parent task's 2 retries.
- **Isolated Failure:** A failure in `sync_settled_data` will not trigger a re-run of the expensive `sync_post_metrics` sweep.
- **Concurrency Control:** Configured with `limit=1` to prevent overlapping runs and API rate-limit collisions.

---

## 3. Architecture & Logical Flow

### 🔄 Data Journey
1.  **Trigger**: Prefect Cron initiates the flow at **8:30 AM** daily.
2.  **Discovery**: `sync_channels` fetches active channel UUIDs.
3.  **Metadata Acquisition**: `sync_post_metrics` sweeps **T-82 → T-2** in 10-day windows against `/publish/items` to capture evolving engagement metrics. (The Prefect task is still named "90-Day Sweep" for historical reasons.)
4.  **Async Polling**: The `BrandwatchClient` initiates asynchronous Insight requests and polls until `READY`.
5.  **Streaming Ingestion**: Large CSV payloads are parsed as a line-generator and inserted in batches of **500 rows**.
6.  **Landing**: All data is committed to the unified staging table `dbo.stg_bw_raw_json`.

### 🏗️ Workflow Diagram
```mermaid
graph TD
    A[Prefect Cron 08:30] --> B{main.py Flow}
    B --> C[task: sync_channels]
    C --> D[task: sync_post_metrics T-82 to T-2]
    D --> E[task: sync_settled_data T-2]
    E --> F[API Async Job Polling]
    F --> G[CSV Streaming / JSON Batching]
    G --> H[(Azure SQL: dbo.stg_bw_raw_json)]
    H --> I[Teams Success/Fail Alert]
```

---

## 4. Observability & Alerting

Integrated with **Microsoft Teams** via Adaptive Cards. The system features proactive monitoring for:
- **SQL Failures**: Connection timeouts (ODBC 18) and insertion errors.
- **API Exhaustion**: Automatic rotation of multiple API keys stored in `.env`.
- **Zombie Run Protection**: A strict **30-minute timeout** on all async polling loops.
- **Async Failures**: Detects and alerts on `FAILED` status within the Brandwatch internal job queue.
- **Flow Summary**: A "Data Engineer's Dream" alert is sent upon successful completion, detailing synced channels and target dates.

---

## 5. Operations

### 🔑 Environment Variables
All config is read from the centralised `/opt/prefect/prod/.env`, mounted read-only into the container.
- `BRANDWATCH_API_KEY*` — any number of prefix-matched keys, rotated round-robin on every request.
- `SQL_SERVER`, `SQL_ORGANICSOCIAL_DATABASE`
- `SQL_USERNAME_INSIGHTLOGIN`, `SQL_PASSWORD_INSIGHTLOGIN` — ⚠️ this pipeline uses the **INSIGHTLOGIN** credentials, **not** the `*_BILOGIN` pair used by the sales/ticketing components.
- `TEAMS_WEBHOOK_OPS`, `TEAMS_WEBHOOK_DEV`, `PREFECT_UI_URL`

### Build & Deploy
```bash
docker compose up -d --build brandwatch-extraction
```

### Monitoring Logs
```bash
docker compose logs -f brandwatch-extraction
```
