# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `dew-insights01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern with Server-Side State  

---

## 1. Project Overview

This service automates financial data extraction from emails using the Microsoft Graph API. It employs a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system is **Stateless Locally**: It tracks processed state directly on the Exchange server using Microsoft Graph categories, ensuring high idempotency and allowing for container-based deployment without local volume persistence.

---

## 2. Directory Structure & Modular Layout

```text
/opt/prefect/prod/code/sales_report_extraction/
├── Dockerfile.sales          # 🐳 Infrastructure: Container definition
├── requirements.txt          # 📦 Infrastructure: Python dependencies
├── main.py                   # 🤖 ORCHESTRATOR: Main Prefect Entrypoint
├── architecture_flow.md      # 🗺️ DOCUMENTATION: High-level system diagram
├── config/                   # ⚙️ CONFIG: JSON routing rules (Timezones, Parsers)
├── data/                     # 💾 STORAGE: Medallion data (Inbox, Processed, Archive)
└── src/                      # 🛠️ APPLICATION: Core logic
    ├── graph_client.py       # MS Graph API (Searching, Downloading, Tagging)
    ├── file_processor.py     # Processing Engine (Timezone logic, Medallion flow)
    ├── sftp_client.py        # SFTP delivery (Paramiko-based)
    └── ...
```

---

## 3. Configuration & State Management

### 🏷️ Server-Side Idempotency (Graph API Tagging)
This system uses the Microsoft Graph API to manage state directly on the email server:
1. **Filtering:** The fetch task specifically filters for emails *without* the `"sales_report_extracted"` category tag.
2. **Tagging:** Once an email is processed, the orchestrator sends a `PATCH` request to apply the `"sales_report_extracted"` tag.
3. **Failure Handling:** If a `ValueError` (like a missing lookup) occurs, the `"sales_report_failed"` tag is applied to prevent zombie retries and trigger a Teams alert.
4. **Robustness:** Includes HTTP 409/412 retry logic to handle Exchange server concurrency conflicts.

### 🌐 Deterministic Timezone Logic
To ensure 100% accurate reporting dates, the engine uses venue-specific timezones defined in `config/show_reporting_rules.json`:
1. It converts the UTC timestamp from the Graph API to the venue's **local timezone** (e.g., `Asia/Singapore`).
2. It subtracts **1 day** (since reports reflect the previous day's sales).
3. This ensures the filename and database entry perfectly align with the local business day regardless of when the email was received.

### 📈 Parameterized Backfills (Prefect UI)
- **`days_back`**: (Default: 30) Controls the dynamic rolling window for untagged email scans.
- **`target_rule_name`**: (Optional) Filters execution to a specific vendor/show rule for historical correction.

---

## 4. Orchestration & Delivery

### 🎯 Granular Task Routing
The pipeline is decomposed into resilient Prefect `@tasks`:
- **`fetch_and_route_emails`**: Handles search and routing with 2 retries for transient API blips.
- **`process_email`**: Encapsulates the full lifecycle of a single file, ensuring that a failure in one vendor report does not crash the entire flow.

### 📤 SFTP Upload Mechanism
Processed files (CSVs or raw passthroughs) are delivered to the Sales Database via a dedicated SFTP client:
- **Streaming Writes:** Uses `f.flush()` and `os.fsync()` to ensure data integrity before upload.
- **Paramiko Integration:** Authenticates via centralized environment variables (`SFTP_SALES_DB_HOST`, etc.) and uploads to the server root.
- **Observability:** Logs exact file sizes (KB) and triggers Teams alerts on delivery failures.

---

## 5. Testing & Validation

To run the tests inside the production container:
```bash
sudo docker exec -it prefect-sales-extraction pytest tests/
```

For more details on writing and mocking tests, see `tests/readme.md`.
