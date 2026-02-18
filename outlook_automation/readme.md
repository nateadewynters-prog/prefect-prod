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
│   └── processed_ids.txt         # History log
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

```
config/show_reporting_rules.json
```

The engine routes emails based on:

- Sender Domain  
- Subject Keyword  
- Explicit Attachment Type (ignoring signature images)

---

### Current Rules

| Rule Name | Trigger | Target Parser | Output Filename Format |
|------------|----------|--------------|-------------------------|
| MALVERN | From: malvern-theatres.co.uk<br>Subj: "Figures"<br>Type: .pdf | malvern_theatre...pdf_parser.py | `{Show}_{Venue}_{ShowID}_{VenueID}_{DocID}_{RecDate}.csv` |
| TICKETEK | From: ticketek.com.sg<br>Subj: "Sales Summary"<br>Type: .xls | ticketek_event...excel_parser.py | `{Show}_{Venue}_{ShowID}_{VenueID}_{DocID}_{RecDate}.csv` |

---

### Strict File Naming & Date Handling

Files are renamed before processing to ensure consistency.

The `{RecDate}` in the filename is:

- Derived from the **Email Received Time in GMT**
- Adjusted to **T-1 (minus 1 day)**
- Formatted as `dd_mm_yy` (2-digit year)

This ensures the filename reflects the **actual sales reporting period**, not the morning it was delivered.

#### Example

If an email was received on **February 18, 2026 (GMT)**:

- Reporting Date (T-1): **February 17, 2026**

- Raw Archive:
  ```
  Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.xls
  ```

- Processed Data:
  ```
  Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.csv
  ```

---

## 4. How to Add a New Show

1. Navigate to `config/show_reporting_rules.json`
2. Add a new block to the `rules` array
3. Ensure you declare the specific `attachment_type`

### Example Configuration

```json
{
    "rule_name": "NEW_SHOW_RULE",
    "active": true,
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

### Historical Backlogs & Smart Search (Predicate Pushdown)

Instead of fetching the top 50 emails and filtering in Python, the script pushes search logic directly to the Microsoft Graph API using the KQL `$search` parameter:

```
from:"domain" AND subject:"keyword"
```

#### Deep Backlogs

- Handles `@odata.nextLink` pagination automatically  
- Can reliably find a 1-year-old email buried under 10,000+ irrelevant emails  

#### Rate Limits

- Detects HTTP `429 Too Many Requests`
- Uses the `Retry-After` header
- Gracefully pauses to avoid Microsoft throttling  

#### Observability

Granular Prefect logs track:

- Page counts  
- Skipped emails  
- Matched emails  

This provides full visibility into backlog processing.

---

### Multiple Attachments Handling

If an email contains multiple attachments:

- The script scans for the file matching the rule’s `attachment_type`
- Skips irrelevant files (e.g., company logos)
- If the required type is missing:
  - A Teams notification is triggered  
  - The email is skipped  

---

### Strict Schema Validation (Data Contracts)

To protect downstream systems, parser functions enforce **strict schema validation**.

If extracted data does not exactly match the expected column structure:

- The script immediately errors  
- The Prefect task fails  
- A Teams alert is sent  
- **No corrupted or misaligned data is saved**

---

### Lookups

If `"needs_lookup": true` is set:

The system searches for:

```
data/lookups/{show_id}_{venue_id}_event_dates.csv
```

It joins this enrichment data with parsed results before saving.

---

## 6. Service Configuration (NSSM)

The service is managed via **NSSM (Non-Sucking Service Manager)**.

- **Service Name:** `outlook-extraction-service`
- **Command:** `C:\Prefect\venv\Scripts\python.exe`
- **Arguments:** `email_extraction_flow.py`
- **Directory:** `C:\Prefect\outlook_automation`
- **Environment Variable:**
  ```
  PREFECT_UI_API_URL=http://10.1.50.126:4200/api
  ```
