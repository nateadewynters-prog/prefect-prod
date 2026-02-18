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
│   ├── lookups\                  # Enrichment data ({ShowID}_{VenueID}_event_dates.csv)
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

### Strict File Naming & Date Handling

Files are renamed *before* processing to ensure consistency. 
The `{RecDate}` in the filename is strictly derived from the **Email Received Time** in **GMT timezone, minus 1 day** (T-1). This ensures the file reflects the actual sales reporting period rather than the morning it was delivered. The date is formatted as a 2-digit year (`dd_mm_yy`). 

* **Example:** If an email was received on **February 18, 2026** (GMT):
* **Reporting Date (T-1):** February 17, 2026
* **Raw Archive:** `Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.xls`
* **Processed Data:** `Jesus_Christ_Superstar_Ticketek_SG_287_220_17_17_02_26.csv`

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

If `needs_lookup: true` is set, the system looks for `data/lookups/{show_id}_event_dates.csv`. It attempts to join this data with the parsed results before saving.

---

## 6. Service Configuration (NSSM)

The service is managed via **NSSM** (Non-Sucking Service Manager) and runs in the same virtual environment as Brandwatch.

* **Service Name:** `outlook-extraction-service`
* **Command:** `C:\Prefect\venv\Scripts\python.exe`
* **Arguments:** `email_extraction_flow.py`
* **Directory:** `C:\Prefect\outlook_automation`
* **Env Vars:** `PREFECT_UI_API_URL=http://10.1.50.126:4200/api`

### PowerShell Installation Script

Run the following in PowerShell as Administrator to install or update the service:

```powershell
$nssm = "C:\Users\batchuser\nssm-2.24-101-g897c7ad\win64\nssm.exe"
$serviceName = "outlook-extraction-service"

# 1. Install Service
& $nssm install $serviceName "C:\Prefect\venv\Scripts\python.exe"

# 2. Set Arguments (Script)
& $nssm set $serviceName AppParameters "email_extraction_flow.py"

# 3. Set Working Directory (Crucial for relative paths)
& $nssm set $serviceName AppDirectory "C:\Prefect\outlook_automation"

# 4. Set Environment Variables (Prefect Reporting)
& $nssm set $serviceName AppEnvironmentExtra "PREFECT_API_URL=[http://10.1.50.126:4200/api](http://10.1.50.126:4200/api)"

# 5. Start Service
Start-Service $serviceName

```

```

```