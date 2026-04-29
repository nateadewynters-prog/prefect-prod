# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `dew-insights01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern with Server-Side State  

---

## 1. Project Overview

This service automates financial data extraction from emails using the Microsoft Graph API. It employs a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system supports two extraction modes:
1. **Attachment-Based:** Standard polling for emails with `.pdf`, `.xls`, or `.xlsx` attachments.
2. **Link-Based:** Scans email HTML bodies for download links, bypasses security redirects (Sophos), hijacks direct AWS S3 "Target URLs", and downloads binary streams directly into the pipeline.

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
    ├── graph_client.py       # MS Graph API (Searching, Body Extraction, Tagging)
    ├── link_downloader.py    # 🔗 Link Extraction (Sophos Bypass, AWS S3 Hijacking)
    ├── file_processor.py     # Processing Engine (Timezone logic, Medallion flow)
    ├── sftp_client.py        # SFTP delivery (Atomic Rename logic)
    └── ...
```

---

## 3. Configuration & State Management

### 🏷️ Server-Side Idempotency (Graph API & Fingerprinting)
This system uses a dual-layer approach to ensure no report is processed twice:
1. **Category Filtering:** The fetch task filters for emails *without* `"sales_report_extracted"`, `"sales_report_failed"`, or `"sales_report_duplicate"` tags.
2. **`internetMessageId` Deduplication:** The orchestrator tracks the unique `internetMessageId` (fingerprint) of every email in the 30-day window. If a duplicate is detected, it is immediately tagged as `"sales_report_duplicate"` and skipped.
3. **Tagging:** Once processed, the `"sales_report_extracted"` tag is applied.
4. **Robustness:** Includes HTTP 409/412 retry logic to handle Exchange server concurrency conflicts during tagging operations.

### 🔗 Extraction Methods
Rules in `config/show_reporting_rules.json` can now toggle the extraction strategy:
- **Default:** If `extraction_method` is omitted, the system looks for attachments.
- **`"extraction_method": "link"`**: Triggers the `link_downloader` to parse the email body, follow redirects, and pull the file from external portals (e.g., Ticketmaster/Sophos).

### 🌐 Deterministic Timezone Logic
To ensure 100% accurate reporting dates, the engine uses venue-specific timezones defined in `config/show_reporting_rules.json`:
1. It converts the UTC timestamp from the Graph API to the venue's **local timezone** (e.g., `Asia/Singapore`).
2. It subtracts **1 day** (since reports reflect the previous day's sales).
3. This ensures the filename and database entry perfectly align with the local business day regardless of when the email was received.

### 🛠️ Universal Logging (Local Testing)
The system uses `get_universal_logger` (from `src.env_setup`) to ensure scripts run seamlessly in both production and local environments. It automatically detects the Prefect context and falls back to a standard Python `StreamHandler` if running outside of a flow, preventing "MissingContext" crashes.

### 📈 Parameterized Execution (Prefect UI)
- **`days_back`**: (Default: 30) Controls the dynamic rolling window for untagged email scans.
- **`target_rule_name`**: (Optional) Filters execution to a specific vendor/show rule for historical correction.
- **`retry_failed`**: (Default: False) When enabled, bulk-resets emails tagged with `"sales_report_failed"` within the `days_back` window, allowing them to be reprocessed.
- **`disable_notifications`**: (Default: False) Silences Microsoft Teams alerts for the duration of the run—ideal for testing or massive backfills.

---

## 4. Orchestration & Delivery

### 🎯 Granular Task Routing
The pipeline is decomposed into resilient Prefect `@tasks`:
- **`fetch_and_route_emails`**: Handles search, routing, and fingerprint-based deduplication with 2 retries.
- **`process_email`**: Encapsulates the full lifecycle of a single file. Utility failures (SFTP, SharePoint) now bubble up to this task, which manages the final tagging and alerting.

### 📤 Dual-Channel Notifications
Alerting is routed based on the target audience using dedicated `.env` variables:
- **Ops Channel (`TEAMS_WEBHOOK_OPS`)**: Receives high-level **Batch Summaries** once the flow completes. To keep the channel clean, these summaries are truncated to show the first 10 successful/review items.
- **Dev Channel (`TEAMS_WEBHOOK_DEV`)**: Receives granular, technical **Error Alerts** (e.g., mapping failures, system exceptions) in real-time.

### 📤 SFTP Upload Mechanism
Processed files are delivered to the Sales Database via a dedicated SFTP client:
- **Streaming Writes:** Uses `f.flush()` and `os.fsync()` to ensure data integrity before upload.
- **Centralized Errors:** Utility-level Teams notifications have been removed; exceptions now bubble up to the orchestrator for consistent error handling.
- **Observability:** Logs exact file sizes (KB) and triggers technical alerts in the Dev channel upon failure.

---

## 5. Testing & Validation

To run the tests inside the production container:
```bash
sudo docker exec -it prefect-sales-extraction pytest tests/
```

For more details on writing and mocking tests, see `tests/readme.md`.
