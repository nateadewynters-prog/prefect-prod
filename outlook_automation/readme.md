# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `dew-insights01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern  

> **System Map Reference:** For global server settings, Docker Compose service management, and Python environment details, refer to the **Master README** at `/opt/prefect/prod/code/readme.md`.

---

## 1. Project Overview

This service automates financial data extraction from emails using the Microsoft Graph API. It uses a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system is **Config-Driven**: Adding a new show or venue does *not* require changing code — only updating `src/outlook_app/config/show_reporting_rules.json`. It runs as a module via the command: `python -m outlook_app.flows.email_extraction_flow`.

---

## 2. Directory Structure & Modular Layout

The project follows a strict `src` layout to separate infrastructure from application logic.

```text
/opt/prefect/prod/code/outlook_automation/
├── Dockerfile.outlook        # 🐳 Infrastructure: Container definition
├── requirements.txt          # 📦 Infrastructure: Python dependencies
├── data/                     # 💾 STORAGE: Stateful Medallion data
│   ├── inbox/                # Temp landing zone
│   ├── processed/            # Final Output CSVs
│   ├── archive/              # Renamed Raw Files (PDF/XLS)
│   ├── failed/               # Error files
│   ├── lookups/              # Enrichment data
│   └── processed_ids.txt     # Audit log
├── src/                      # 🛠️ APPLICATION: Core logic
│   └── outlook_app/
│       ├── clients/          # Pure API authentication and extraction
│       ├── config/           # JSON routing rules
│       ├── core/             # Business logic and state management
│       ├── flows/            # Prefect orchestrators
│       ├── parsers/          # Extraction logic (PDF/Excel)
│       └── utils/            # Helper modules (DB, Env, Notifications)
└── tests/                    # 🧪 TESTING: Isolated test suite
```

### Module Responsibilities

- **`clients/graph_client.py`**: Handles pure Microsoft Graph API interaction, including authentication, email searching, and attachment downloading.
- **`core/file_processor.py`**: Manages the "brain" of the operation—business logic, lookup merging, Medallion file I/O, and updating the stateful JSON configuration.
- **`flows/email_extraction_flow.py`**: A lean Prefect orchestrator that ties the clients and core logic together into a scheduled workflow.

---

## 3. Configuration & Routing

All logic is controlled by:
`src/outlook_app/config/show_reporting_rules.json`

The engine routes emails based on:
- Sender Domain  
- Subject Keyword  
- Explicit Attachment Type  

### Stateful Backfilling & Progress Tracking
Each rule in the JSON config tracks its own progress using `"backfill_since": "YYYY-MM-DD"`. After successful processing, the system automatically advances this date, eliminating redundant API calls and enabling seamless historical backfills.

---

## 4. Testing

This project includes an isolated test suite to verify client connectivity and processing logic without affecting production data.

To run the tests:

```bash
# Navigate to the project root
cd /opt/prefect/prod/code/outlook_automation

# Run pytest with the src directory in the PYTHONPATH
export PYTHONPATH=$(pwd)/src && pytest tests/ -v
```

---

## 5. Technical Details

### Dynamic Parser Loading
The pipeline resolves parser code references declared in the configuration at runtime using Python's `importlib`. This allows the system to scale to hundreds of vendors without bloating the main orchestrator.

### Strict Schema Validation & Data Contracts
To protect downstream systems, parsers enforce strict schema validation via a **Generic Validation Pattern** using the `ValidationResult` dataclass (defined in `src/outlook_app/core/models.py`).

Each parser returns:
1. **Parsed Data** (List of Dicts or DataFrame)
2. **ValidationResult** (Status, Message, and Metrics)

### Orchestrator Behavior & Teams Integration
The system integrates with Microsoft Teams via Adaptive Card Webhooks. 
- `FAILED`: Hard failure. File moved to `failed/`. Red ❌ Teams alert.
- `UNVALIDATED`: Warning ⚠️ Teams alert. Manual review required.
- `PASSED`: Silently logged.

---

## ✅ Architectural Guarantees

- **Infrastructure/Code Separation**: Docker and requirements are decoupled from the `src` logic.
- **Modularized Utilities**: No more monolithic `utils.py`; logic is split into `database.py`, `notifications.py`, etc.
- **Config-driven**: Idempotent and observable via Prefect Artifacts.
