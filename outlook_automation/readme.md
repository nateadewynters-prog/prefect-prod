# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `DEW-DBSYNC01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern  

> **System Map Reference:** For global server settings, NSSM service management, and Python environment details, refer to the **Master README** at `C:\Prefect\README.md`.

---

## 1. Project Overview

This service automates financial data extraction from emails. It uses a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system is **Config-Driven**: Adding a new show or venue does *not* require changing code — only updating `config/show_reporting_rules.json`.

---

## 2. Directory Structure

```text
C:\Prefect\outlook_automation\
├── config\                       # ⚙️ BRAIN
│   └── show_reporting_rules.json # Defines routing & naming rules
├── data\                         # 💾 STORAGE
│   ├── inbox\                    # Temp landing zone
│   ├── processed\                # Final Output CSVs
│   ├── archive\                  # Renamed Raw Files (PDF/XLS)
│   ├── failed\                   # Error files
│   ├── lookups\                  # Enrichment data ({ShowID}_{VenueID}_event_dates.csv)
│   └── processed_ids.txt         # Audit log CSV (timestamp, msg_id, rule_name)
├── parsers\                      # 🧠 LOGIC
│   ├── __init__.py
│   ├── ticketek_event_settlement_excel_parser.py        
│   └── malvern_theatre_contractual_report_pdf_parser.py 
└── email_extraction_flow.py      # Main Orchestrator
```

---

### 🔗 External Dependencies

- `C:\Prefect\.env`  
  Master credentials (Azure Tenant/Client IDs, Webhooks)

- `C:\Prefect\shared_lib\utils.py`  
  Unified environment loading, Data Contracts, and Teams notification logic

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

---

### Stateful Backfilling & Progress Tracking

Each rule in the JSON config tracks its own progress using:

```json
"backfill_since": "YYYY-MM-DD"
```

When the script runs:

1. It searches for relevant emails  
2. Uses Python to strictly filter items received on or after this date  
3. After successful processing, it automatically updates the JSON file  
4. The `backfill_since` value advances forward  

This design:

- Eliminates redundant API calls  
- Enables seamless historical backfills  
- Allows years of history to be reprocessed by simply adjusting the date  

---

### Strict File Naming & Date Handling

Files are renamed **before processing** to ensure consistency.

The `{RecDate}` is:

- Derived from Email Received Time (GMT)  
- Adjusted to **T-1** (minus 1 day)  
- Formatted as `dd_mm_yy`  

This guarantees:

- Reporting-date accuracy  
- Timezone consistency  
- Deterministic downstream file naming  

---

## 4. How to Add a New Show

1. Navigate to:

```
config/show_reporting_rules.json
```

2. Add a new block to the `rules` array  
3. Declare:
   - `attachment_type`
   - `backfill_since` starting date  

4. Restart the service:

```powershell
Restart-Service "outlook-extraction-service"
```

No Python code changes are required.

---

## 5. Technical Details

---

### Strict Schema Validation & Data Contracts

To protect downstream systems, parsers enforce strict schema validation via a **Generic Validation Pattern** using the `ValidationResult` dataclass.

Each parser returns two objects:

1. **Parsed Data**  
   Structured dictionary (or DataFrame) of extracted rows  

2. **ValidationResult**  
   A dataclass containing:

   - `status` → `PASSED`, `FAILED`, or `UNVALIDATED`  
   - `message` → Human-readable explanation  
   - `metrics` → Flexible dictionary of extracted metrics  
     - Example: `{"Extracted Tickets": 1500}`  

---

### Orchestrator Behavior

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

### Hybrid Search Strategy (Microsoft Graph API)

The script uses a **Hybrid Strategy** to bypass Graph limitations:

1. **Fuzzy API Search**  
   Pushes a targeted text search directly to Graph using `$search`.

2. **Strict Python Date Filter**  
   Evaluates the `backfill_since` boundary locally in Python.

3. **Deep Backlogs & Rate Limits**  
   - Handles `@odata.nextLink` pagination automatically  
   - Gracefully pauses when receiving `429 Too Many Requests`  

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
