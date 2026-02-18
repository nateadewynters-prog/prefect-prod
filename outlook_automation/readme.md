Here is your cleaned, properly structured, and export-ready `readme.md` wrapped in a single Markdown code block:

````markdown
# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `DEW-DBSYNC01`  
**Status:** 🟢 Active (Polling every 15m)  
**Architecture:** JSON-Driven Medallion Pattern  

---

## 1. Project Overview

This service automates financial data extraction from emails. It uses a **Medallion Architecture** to separate raw data, processing logic, and final curated outputs.

The system is **Config-Driven**: Adding a new show or venue does *not* require changing code, only updating `config/show_reporting_rules.json`.

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
├── outlook_utils.py              # Shared Utils (Teams Webhooks)
└── email_extraction_flow.py      # Main Orchestrator
```

---

## 3. Configuration & Routing

All logic is controlled by:

`config/show_reporting_rules.json`

The engine routes emails based on:

- Sender Domain  
- Subject Keyword  
- Explicit Attachment Type  

---

### Stateful Backfilling & Progress Tracking

Each rule in the JSON config tracks its own progress using:

`"backfill_since": "YYYY-MM-DD"`

When the script runs:

- It searches for relevant emails and uses Python to strictly filter for items received **on or after** this date.
- After successful processing, it automatically updates the JSON file.
- The `backfill_since` value advances forward.

This:

- Eliminates redundant API calls  
- Enables seamless historical backfills  
- Allows years of history to be reprocessed by simply adjusting the date  

---

### Current Rules

| Rule Name | Trigger | Target Parser | Output Filename Format |
|------------|----------|--------------|-------------------------|
| MALVERN | From: malvern-theatres.co.uk<br>Subj: "Figures"<br>Type: .pdf | malvern_theatre...pdf_parser.py | `{Show}_{Venue}_{ShowID}_{VenueID}_{DocID}_{RecDate}.csv` |
| TICKETEK | From: ticketek.com.sg<br>Subj: "Sales Summary"<br>Type: .xls | ticketek_event...excel_parser.py | `{Show}_{Venue}_{ShowID}_{VenueID}_{DocID}_{RecDate}.csv` |

---

### Strict File Naming & Date Handling

Files are renamed before processing to ensure consistency.

The `{RecDate}`:

- Derived from **Email Received Time (GMT)**
- Adjusted to **T-1 (minus 1 day)**
- Formatted as `dd_mm_yy`

This ensures filenames reflect the **actual sales reporting period**, not delivery time.

#### Example

If email received: **February 18, 2026 (GMT)**  
Reporting Date (T-1): **February 17, 2026**

Raw Archive:  
`Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.xls`

Processed Data:  
`Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.csv`

---

## 4. How to Add a New Show

1. Navigate to `config/show_reporting_rules.json`
2. Add a new block to the `rules` array
3. Declare:
   - `attachment_type`
   - `backfill_since` starting date

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

4. Restart the service.

---

## 5. Technical Details

### Audit Logging (`processed_ids.txt`)

While the JSON state file controls how far back to search, `processed_ids.txt` acts as the **final deduplication fail-safe**.

It is a CSV-formatted audit log recording:

`timestamp, msg_id, rule_name`

If a message ID exists in this file:

- It is permanently skipped  
- Duplicate extraction is prevented  

---

### Historical Backlogs & Hybrid Search Strategy

Instead of downloading massive inboxes or fighting Microsoft Graph's `$filter` restrictions, the script uses a **Hybrid Strategy**:

- **Fuzzy API Search:**  
  Pushes a targeted text search directly to Graph using the `$search` parameter (e.g., `"domain keyword"`). This instantly bypasses irrelevant emails.

- **Strict Python Date Filter:**  
  Because Microsoft blocks combining `$search` and `$filter`, the `backfill_since` date boundary is evaluated locally in Python. Older emails are instantly dropped.

- **Deep Backlogs:**  
  Handles `@odata.nextLink` pagination automatically, allowing retrieval of emails buried under 10,000+ irrelevant messages.

- **Rate Limits:**  
  Detects HTTP `429 Too Many Requests`, uses the `Retry-After` header, and gracefully pauses to avoid throttling.

---

### Multiple Attachments Handling

If an email contains multiple attachments:

- Scans for file matching rule’s `attachment_type`
- Skips irrelevant files (e.g., logos)
- If required type missing:
  - Sends Teams notification
  - Skips email

---

### Strict Schema Validation (Data Contracts)

To protect downstream systems, parsers enforce strict schema validation.

If extracted data does not exactly match expected column structure:

- Script errors immediately  
- Prefect task fails  
- Teams alert is sent  
- **No corrupted or misaligned data is saved**
````
