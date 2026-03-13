# 🛠️ Application Source (src)

**Domain:** Core Business Logic & Infrastructure  
**Structure:** Modular Service Pattern  

---

## 1. Overview

This directory contains the application logic, decoupled from the orchestration layer.

---

## 2. Component Layout

### 📡 API & External Clients
- **`graph_client.py`**: Specialized client for Microsoft Graph API. Handles OIDC/MSAL authentication, **subject-only keyword searching**, attachment downloading, and **category tagging** for state management. Features `tag_email` and `untag_email` methods with HTTP 409/412 retry logic to handle Exchange server conflicts.
- **`sftp_client.py`**: A `paramiko`-based client for delivering processed CSVs or raw passthrough files. It logs file size (KB), utilizes centralized environment variables, and includes error alerting for Teams.

### 🧠 Core Engine
- **`file_processor.py`**: The `ProcessingEngine` class. Manages the full file lifecycle:
    - **Deterministic Report Dating:** Converts UTC to local venue time via `pytz` and standardizes dates.
    - **Medallion I/O:** Standardizes filenames and moves files across zones (`inbox` -> `archive`/`processed`/`failed`).
    - **Dynamic Parser Invocation:** Uses `importlib` to route files to specialized parsers.
    - **Passthrough Logic:** Routes raw attachments directly for rules configured with `"passthrough_only": true`.
    - **Failure Handling:** Moves problematic files to the `failed/` zone.

### 🧱 Shared Models & Utilities
- **`models.py`**: Unified Data Contracts (e.g., `ValidationResult`).
- **`database.py`**: Shared logic for internal databases.
- **`env_setup.py`**: Centralized environment variable loader.
- **`notifications.py`**: Microsoft Teams Adaptive Card logic for alerting. Features a `disable_notifications` toggle for silent runs.

---

## 3. Design Principles

1. **Stateless Logic:** The system relies on Graph tags and a **30-day dynamic rolling window**, ensuring it remains stateless locally.
2. **Robust Retrieval:** Employs a simplified, subject-only keyword search to bypass KQL query limitations, with sender validation handled purely in Python.
3. **Data Integrity:** Employs `f.flush()` and `os.fsync()` before SFTP uploads to prevent 0-byte file delivery.
4. **Resilient Tagging:** Exchange server conflicts are mitigated with automatic retries for HTTP 409/412 responses during tagging and untagging.
5. **Silent Mode:** Support for `disable_notifications` allows for high-volume backfills or testing without flooding Teams channels.
