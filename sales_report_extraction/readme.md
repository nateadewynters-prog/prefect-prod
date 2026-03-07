# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `dew-insights01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern with Server-Side State  

---

## 1. Project Overview

This service automates financial data extraction from emails using the Microsoft Graph API. It employs a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system is **Stateless Locally**: It does not track progress in local JSON files. Instead, it uses a 30-day dynamic rolling window and tracks processed state directly on the Exchange server using Microsoft Graph categories, ensuring high idempotency.

---

## 2. Directory Structure & Modular Layout

The project follows a strict `src` layout to separate infrastructure from application logic.

```text
/opt/prefect/prod/code/sales_report_extraction/
├── Dockerfile.sales          # 🐳 Infrastructure: Container definition
├── requirements.txt          # 📦 Infrastructure: Python dependencies
├── main.py                   # 🤖 ORCHESTRATOR: Main Prefect Entrypoint
├── architecture_flow.md      # 🗺️ DOCUMENTATION: High-level system diagram
├── config/                   # ⚙️ CONFIG: JSON routing rules
├── data/                     # 💾 STORAGE: Stateful Medallion data
│   ├── inbox/                # Temp landing zone
│   ├── processed/            # Final Output (CSVs or Raw Passthrough)
│   ├── archive/              # Renamed Raw Files (PDF/XLS)
│   ├── failed/               # Error files (Quarantine)
│   └── lookups/              # Enrichment data (Event date mappings)
├── src/                      # 🛠️ APPLICATION: Core logic
│   ├── graph_client.py       # API authentication, searching, and tagging
│   ├── file_processor.py     # Business logic and Medallion I/O
│   ├── models.py             # Data Contracts (ValidationResult)
│   ├── database.py           # Shared internal utilities (DB)
│   ├── env_setup.py          # Environment loading
│   ├── notifications.py      # MS Teams notification logic
│   ├── sftp_client.py        # SFTP delivery client
│   └── parsers/              # Extraction logic (PDF/Excel)
└── tests/                    # 🧪 TESTING: Isolated test suite
```

---

## 3. Configuration & State Management

### 📡 Routing Logic
All logic is controlled by `config/show_reporting_rules.json`. The engine routes emails based on:
- **Subject Keyword:** A simplified, robust subject-only search via Graph API.
- **Sender Domain:** Validated purely in Python after retrieval for maximum reliability. 
- **Explicit Attachment Type:** Strict extension enforcement (e.g., `.pdf`, `.xls`).

### 🏷️ Server-Side Idempotency
This system uses the Microsoft Graph API to manage state directly on the email server:
1. **Filtering:** The fetch task specifically filters for emails *without* the `"sales_report_extracted"` category tag.
2. **Tagging:** Once an email is processed (successfully or with a handled error), the orchestrator sends a `PATCH` request to apply the `"sales_report_extracted"` tag to the message on the server.
3. **Robustness:** The tagging logic includes HTTP 409/412 retry logic to handle Exchange server concurrency conflicts.

### 📈 Parameterized Backfills (Prefect UI)
Local state tracking (`backfill_since`) has been removed in favor of a 30-day dynamic rolling window. For historical runs, the Prefect 3.0 flow accepts UI parameters:
- **`days_back`**: (Default: 30) Controls how far back the system scans for untagged emails.
- **`target_rule_name`**: (Optional) Allows a "Custom Run" to target a specific vendor/show rule.

### ⏩ Passthrough Feature
Rules can be configured with `"passthrough_only": true` in the `processing` section. When enabled:
- The engine skips Pandas/CSV extraction.
- The raw attachment is moved directly to the `data/processed/` directory.
- The raw file is uploaded to SFTP in its original format.
- The email is tagged as successful.

---

## 4. Testing & Validation

This project includes a comprehensive test suite using `pytest`.

To run the tests inside the production container:
```bash
sudo docker exec -it prefect-sales-extraction pytest tests/
```

For more details on writing and mocking tests, see `tests/readme.md`.

---

## 5. Technical Highlights

- **Simplified Search:** Uses a subject-only KQL query to avoid Graph API "AND" logic limitations, with secondary validation in Python.
- **Dynamic Parser Loading:** Resolves parser code at runtime via `importlib`.
- **Strict Data Contracts:** Parsers return a `ValidationResult` (Status, Message, Metrics) to ensure observability.
- **Medallion Movement:** Raw files are moved from `inbox` to `archive` (on success) or `failed` (on error).
- **Data Integrity:** Employs `f.flush()` and `os.fsync()` before SFTP uploads to prevent 0-byte file errors.
