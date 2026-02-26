# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `dew-insights01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern  

> **System Map Reference:** For global server settings, Docker Compose service management, and Python environment details, refer to the **Master README** at `/opt/prefect/prod/code/readme.md`.

---

## 1. Project Overview

This service automates financial data extraction from emails using the Microsoft Graph API. It uses a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system is **Config-Driven**: Adding a new show or venue does *not* require changing code — only updating `config/show_reporting_rules.json`. It runs as a module via the command: `python -m outlook_automation.email_extraction_flow`.

---

## 2. Directory Structure

```text
/opt/prefect/prod/code/outlook_automation/
├── config/                       # ⚙️ BRAIN
│   └── show_reporting_rules.json # Defines routing & naming rules
├── data/                         # 💾 STORAGE
│   ├── inbox/                    # Temp landing zone
│   ├── processed/                # Final Output CSVs
│   ├── archive/                  # Renamed Raw Files (PDF/XLS)
│   ├── failed/                   # Error files
│   ├── lookups/                  # Enrichment data ({ShowID}_{VenueID}_event_dates.csv)
│   └── processed_ids.txt         # Audit log CSV (timestamp, msg_id, rule_name)
├── parsers/                      # 🧠 LOGIC
│   ├── __init__.py
│   ├── ticketek_event_settlement_excel_parser.py        
│   └── malvern_theatre_contractual_report_pdf_parser.py 
└── email_extraction_flow.py      # Main Orchestrator
```

---

### 🔗 Dependencies

- **/opt/prefect/prod/.env** (Volume Mounted)  
  Master credentials (Azure Tenant/Client IDs, Webhooks)

- **./utils.py** (Localized)  
  Environment loading, Data Contracts, and Teams notification logic

- **./requirements.txt** (Localized)  
  Specific Python dependencies for the Outlook automation service

---

## 3. Configuration & Routing

All logic is controlled by:

```
config/show_reporting_rules.json
```

The engine routes emails based on:

- Sender Domain  
- Subject Keyword  
- Explicit Attachment Type  

### The `fetch_and_route_emails` Task
This primary Prefect task performs a hybrid search strategy to locate emails matching active rules in the configuration. It uses a fuzzy Graph API search (`$search`) combined with strict local Python date filtering to securely boundary messages. Valid emails are queued up for attachment processing.

---

### Stateful Backfilling & Progress Tracking

Each rule in the JSON config tracks its own progress using:

```json
"backfill_since": "YYYY-MM-DD"
```

When the script runs:

1. It searches for relevant emails via `fetch_and_route_emails`.
2. Uses Python to strictly filter items received on or after this date.
3. After successful processing, it automatically updates the JSON file.
4. The `backfill_since` value advances forward.

This design:

- Eliminates redundant API calls  
- Enables seamless historical backfills  
- Allows years of history to be reprocessed by simply adjusting the date  

---

## 4. Data Flow Pathing

The application enforces a Medallion-style data movement structure:

1. **Inbox:** Target attachments are extracted and temporarily stored in `data/inbox/`.
2. **Processing:** The corresponding parser dynamically unpacks the file.
3. **Processed/Archive:** Successfully generated CSVs are stored in `data/processed/`, and the original files are moved to `data/archive/`.
4. **Failed:** If a file fails parsing or validation, the raw file is moved to `data/failed/` for review.

---

## 5. Technical Details

---

### Dynamic Parser Loading

The pipeline resolves parser code references declared in `config/show_reporting_rules.json` at runtime using Python's `importlib`.

```python
parser_module = importlib.import_module(proc_config['parser_module'])
parser_function = getattr(parser_module, proc_config['parser_function'])
```

This prevents the orchestrator (`email_extraction_flow.py`) from becoming bloated with hardcoded import statements for every specific venue or event template.

---

### Strict Schema Validation & Data Contracts

To protect downstream systems, parsers enforce strict schema validation via a **Generic Validation Pattern** using the `ValidationResult` dataclass (defined in `utils.py`).

Each parser returns two objects:

1. **Parsed Data**  
   Structured dictionary (or DataFrame) of extracted rows  

2. **ValidationResult**  
   A dataclass containing:

   - `status` → `PASSED`, `FAILED`, or `UNVALIDATED`  
   - `message` → Human-readable explanation  
   - `metrics` → Flexible dictionary of extracted metrics  
     - Example: `{"Calculated Tickets": 1500}`  

---

### Orchestrator Behavior & Teams Integration

The system natively integrates with Microsoft Teams via an Adaptive Card Webhook defined in `utils.py`.

| Status        | System Action |
|--------------|--------------|
| `FAILED`      | Hard failure. File moved to `failed/`. Red ❌ Teams alert sent. |
| `UNVALIDATED` | Warning ⚠️ Teams alert. Manual review required. |
| `PASSED`      | Silently logged to console (prevents alert fatigue). |

All validation results automatically generate a **Prefect Markdown Artifact**, enabling analysts to review extracted metrics directly inside the Prefect UI.

---

### Audit Logging (`processed_ids.txt`)

While the JSON state file controls historical boundaries, `processed_ids.txt` acts as the **final deduplication fail-safe**.

It is a CSV-formatted audit log recording:

```
timestamp, msg_id, rule_name
```

If a message ID exists in this file, it is permanently skipped.

---

## ✅ Architectural Guarantees

This pipeline is:

- Config-driven  
- Stateful  
- Idempotent  
- Timezone-safe (GMT standardized)  
- Strictly validated (Data Contracts)  
- Fully observable via Prefect Artifacts  
- Centralized for credentials and notifications  