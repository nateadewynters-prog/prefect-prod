# 🛠️ Application Source (src)

**Domain:** Core Business Logic & Infrastructure  
**Structure:** Modular Service Pattern  

---

## 1. Overview

This directory contains the application logic, decoupled from the orchestration layer.

---

## 2. Component Layout

### 📡 API & External Clients
- **`graph_client.py`**: Specialized client for Microsoft Graph API. Handles OIDC/MSAL authentication, **subject-only keyword searching**, attachment downloading (optionally picking one of several attachments via `filename_keyword`), and **category tagging** for state management. Features `tag_email` and `untag_email` methods with HTTP 409/412 retry logic to handle Exchange server conflicts. Now includes `internetMessageId` extraction for fingerprint-based deduplication.
- **`link_extractor.py`**: Extracts and downloads files from HTML links within email bodies, used primarily for Ticketmaster-based reports where attachments are hosted externally.
- **`sftp_client.py`**: A `paramiko`-based client for delivering processed CSVs or raw passthrough files. It logs file size (KB) and utilizes centralized environment variables. Notifications have been removed; exceptions now bubble up to the orchestrator.
- **`sharepoint_uploader.py`**: Handles file uploads to the Medallion SharePoint site. Notifications have been removed; exceptions bubble up to the orchestrator.
- **`config_loader.py`**: The `SharePointRuleLoader` class. Fetches every routing rule fresh from a SharePoint List via the Graph API at the start of each flow run, reshaping list items into the rule dicts the pipeline expects. Validates each row as it goes, skipping (and logging) rows with a missing show/venue, a blank `SubjectKeyword` or `SenderDomain`, a `rule_name` that duplicates an earlier row, or only one of the two parser columns filled — see `config/readme.md` for the full table. The blank-cell and duplicate checks apply to **active** rows only, so a parked row being prepared for a future show never blocks a live one. Also provides `update_last_run`, which stamps the `LastRun` column on a rule's row after a successful extraction (best-effort — failures are logged, not raised).

### 🧠 Core Engine
- **`file_processor.py`**: The `ProcessingEngine` class. Manages the full file lifecycle:
    - **Deterministic Report Dating:** Converts UTC to local venue time via `pytz` and standardizes dates, incorporating **Sales Day Offset** logic for late-night reports.
    - **Medallion I/O:** Standardizes filenames and moves files across zones (`inbox` -> `archive`/`processed`/`failed`).
    - **Dynamic Parser Invocation:** Uses `importlib` to route files to specialized parsers. The processed file's format follows the parser's optional `OUTPUT_EXT` constant (e.g. `.xlsx`), defaulting to `.csv`.
    - **Passthrough Logic:** Routes raw attachments directly for rules where the SharePoint List's `ParserModule`/`ParserFunction` columns are left blank.
    - **Failure Handling:** Moves problematic files to the `failed/` zone. Notifications have been removed; exceptions bubble up.
- **`naming.py`**: Centralized logic for generating standardized, deterministic filenames based on show/venue metadata and reporting dates.
- **`mapping.py`**: Handles data transformation and lookups, mapping vendor-specific codes to internal identifiers using local CSV tables.

### 🧱 Shared Models & Utilities
- **`models.py`**: Unified Data Contracts (e.g., `ValidationResult`).
- **`database.py`**: SQL Server connection helper (`pyodbc`, `SQL_*` env vars). ⚠️ **Currently unused** — no module in this component imports it.
- **`error_db_client.py`**: Specialized client for logging mapping and lookup failures to the central `dataops_tracking.db` for later review.
- **`env_setup.py`**: Centralized environment variable loader. Includes **`get_universal_logger`** with an automatic fallback to standard Python logging for local testing.
- **`notifications.py`**: Microsoft Teams Adaptive Card logic for alerting. Features **Dual-Channel Routing** (Ops vs. Dev) via `TEAMS_WEBHOOK_OPS` and `TEAMS_WEBHOOK_DEV` environment variables. Includes a `disable_notifications` toggle for silent runs.

---

## 3. Design Principles

1. **Stateless Logic:** The system relies on Graph tags and a **dynamic rolling window** (`days_back`, default 7 days), ensuring it remains stateless locally.
2. **Robust Retrieval:** Employs a simplified, subject-only keyword search to bypass KQL query limitations, with sender validation handled purely in Python.
3. **Data Integrity:** Employs `f.flush()` and `os.fsync()` before SFTP uploads to prevent 0-byte file delivery.
4. **Resilient Tagging:** Exchange server conflicts are mitigated with automatic retries for HTTP 409/412 responses during tagging and untagging.
5. **Silent Mode:** Support for `disable_notifications` allows for high-volume backfills or testing without flooding Teams channels.
