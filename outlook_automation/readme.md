# 📧 Medallion Email Extraction Automation (Prefect 3.0)

**Host:** `DEW-DBSYNC01`
**Status:** 🟢 Active (Polling every 60s)
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
│   ├── lookups\                  # Enrichment data ({ShowID}_event_dates.csv)
│   └── processed_ids.txt         # History log
├── parsers\                      # 🧠 LOGIC
│   ├── __init__.py
│   ├── ticketek_event_settlement_excel_parser.py        # Was sistic_agency_parser
│   └── malvern_theatre_contractual_report_pdf_parser.py # Was malvern_theatre_parser
├── outlook_utils.py              # Shared Utils
└── email_extraction_flow.py      # Main Orchestrator

```

---

## 3. Configuration & Routing

All logic is controlled by `config/show_reporting_rules.json`.

### Current Rules

| Rule Name | Trigger | Target Parser | Output Filename Format |
| --- | --- | --- | --- |
| **MALVERN** | **From:** `malvern-theatres.co.uk`<br>

<br>**Subj:** "Figures" | `malvern_theatre...pdf_parser.py` | `{Show}_{Venue}_{ShowID}_{VenueID}_{DocID}_{RecDate}.csv` |
| **TICKETEK** | **From:** `ticketek.com.sg`<br>

<br>**Subj:** "Sales Summary" | `ticketek_event...excel_parser.py` | `{Show}_{Venue}_{ShowID}_{VenueID}_{DocID}_{RecDate}.csv` |

### Example Filename Output

If an email was received on **October 15, 2023**:

* **Raw Archive:** `Jesus Christ Superstar_Ticketek SG_287_220_17_15_10_23.xls`
* **Processed Data:** `Jesus Christ Superstar_Ticketek SG_287_220_17_15_10_23.csv`

---

## 4. How to Add a New Show

1. Navigate to `config/show_reporting_rules.json`.
2. Add a new block to the `rules` array:

```json
{
    "rule_name": "NEW_SHOW_RULE",
    "active": true,
    "match_criteria": {
        "sender_domain": "new-venue.com",
        "subject_keyword": "Settlement"
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

3. Restart the service.

---

## 5. Technical Details

### Dynamic Parser Loading

The orchestrator uses Python's `importlib` to load the parser specified in the JSON string `parser_module`. This means you can swap parsers without restarting the flow if the code is updated.

### Date Handling

The filename date (`15_10_23`) is derived strictly from the **Email Received Time** (via Graph API `receivedDateTime`), ensuring that re-running the script on an old email yields the exact same filename.

### Lookups

If `needs_lookup: true` is set, the system looks for:
`data/lookups/{show_id}_event_dates.csv`
It attempts to join this data with the parsed results before saving.

```

```