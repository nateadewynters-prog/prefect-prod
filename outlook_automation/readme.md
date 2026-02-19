# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `DEW-DBSYNC01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern  

> **System Map Reference:** For global server settings, NSSM service management, and Python environment details, refer to the **Master README** at `C:\Prefect\README.md`.

---

## 1. Project Overview

This service automates financial data extraction from emails. It uses a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system is **Config-Driven**: Adding a new show or venue does *not* require changing code — only updating:

```
config/show_reporting_rules.json
```

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

### 🔗 External Dependencies

```
C:\Prefect\.env                  # Master credentials (Azure Tenant/Client IDs, Webhooks)
C:\Prefect\shared_lib\utils.py   # Unified environment loading and Teams notification logic
```

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

```
"backfill_since": "YYYY-MM-DD"
```

When the script runs:

- It searches for relevant emails
- Uses Python to strictly filter items received **on or after** this date
- After successful processing, it automatically updates the JSON file
- The `backfill_since` value advances forward

This:

- Eliminates redundant API calls  
- Enables seamless historical backfills  
- Allows years of history to be reprocessed by simply adjusting the date  

---

### Strict File Naming & Date Handling

Files are renamed before processing to ensure consistency.

The `{RecDate}` is:

- Derived from **Email Received Time (GMT)**
- Adjusted to **T-1 (minus 1 day)**
- Formatted as `dd_mm_yy`

This ensures filenames reflect the **actual sales reporting period**, not delivery time.

---

#### Example

If email received:

```
February 18, 2026 (GMT)
```

Reporting Date (T-1):

```
February 17, 2026
```

Raw Archive:

```
Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.xls
```

Processed Data:

```
Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.csv
```

---

## 4. How to Add a New Show

1. Navigate to:

```
config/show_reporting_rules.json
```

2. Add a new block to the `rules` array.

3. Declare:
   - `attachment_type`
   - `backfill_since` starting date

---

### Example Configuration

```json
{
    "rule_name": "NEW_SHOW_RULE",
    "active": true,
    "backfill_since": "2023-01-01",
    "match_criteria": {
        "sender_domain": "new-venue.com",
        "subject_keyword": "Settlement",
        "attachment_type": ".xlsx"
    },
    "metadata": {
        "show_name": "New Musical",
        "venue_name": "The Globe",
        "show_id": "999",
        "venue_id": "VN-GLOBE",
        "document_id": "17"
    },
    "processing": {
        "parser_module": "parsers.ticketek_event_settlement_excel_parser",
        "parser_function": "extract_settlement_data",
        "needs_lookup": true
    }
}
```

4. Restart the service:

```powershell
Restart-Service "outlook-extraction-service"
```

---

## 5. Technical Details

### Unified Teams Notifications

Alerts for:

- Extraction successes  
- Attachment mismatches  
- Parser failures  

Are routed through the standardized:

```
send_teams_notification()
```

Located in:

```
C:\Prefect\shared_lib\utils.py
```

This ensures the payload strictly matches the **Adaptive Card schema** expected by the Power Automate workflow.

---

### Audit Logging (`processed_ids.txt`)

While the JSON state file controls how far back to search, `processed_ids.txt` acts as the **final deduplication fail-safe**.

It is a CSV-formatted audit log recording:

```
timestamp, msg_id, rule_name
```

If a message ID exists in this file:

- It is permanently skipped  
- Duplicate extraction is prevented  

---

### Hybrid Search Strategy (Microsoft Graph API)

Instead of downloading massive inboxes or fighting Microsoft Graph's `$filter` restrictions, the script uses a **Hybrid Strategy**:

**Fuzzy API Search**
- Pushes a targeted text search directly to Graph using the `$search` parameter  
- Example: `"domain keyword"`  
- Instantly bypasses irrelevant emails  

**Strict Python Date Filter**
- Microsoft blocks combining `$search` and `$filter`  
- The `backfill_since` boundary is evaluated locally in Python  
- Older emails are instantly dropped  

**Deep Backlogs & Rate Limits**
- Handles `@odata.nextLink` pagination automatically  
- Gracefully pauses when hitting `429 Too Many Requests`  
- Respects the `Retry-After` header  

---

### Strict Schema Validation (Data Contracts)

To protect downstream systems, parsers enforce strict schema validation.

If extracted data does **not exactly match** the expected column structure:

- The script errors immediately  
- The Prefect task fails  
- A Teams alert is sent  
- **No corrupted or misaligned data is saved**  

---

## ✅ Architectural Guarantees

This pipeline is:

- Config-driven  
- Stateful  
- Idempotent  
- Timezone-safe (GMT standardized)  
- Strictly validated  
- Fully observable via Prefect  
- Centralized for credentials and notifications  

It is production-hardened for long-term automated financial reporting.
