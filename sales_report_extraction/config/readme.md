# ⚙️ Configuration & Routing Rules

**Domain:** Orchestration & Data Mapping  
**Format:** JSON-Driven Medallion Pattern  

---

## 1. Overview

This directory contains the central configuration layer. All email routing, medallion mapping, and parser assignments are handled via declarative JSON rules.

---

## 2. Rule Structure (`show_reporting_rules.json`)

Each object in the `"rules"` array controls one unique data flow:

### 📡 Match Criteria
- **`sender_domain`**: The verified source domain of the email.
- **`subject_keyword`**: String used for simplified, robust subject-only keyword search in the Graph API.
- **`attachment_type`**: Strict extension enforcement (e.g., `.pdf`, `.xls`).

### 🏷️ Metadata Mapping (Medallion)
- **`show_name`**, **`venue_name`**: Used for standard filename generation.
- **`show_id`**, **`venue_id`**, **`document_id`**: Identifiers for downstream systems.
- **`timezone`**: The exact IANA Time Zone (e.g., `Asia/Singapore`, `Europe/London`).

### 🌐 Deterministic Timezone Logic
The engine uses the `timezone` field to perfectly align the UTC email receipt time with the venue's local reporting date:
1. **Conversion:** UTC (from MS Graph) is converted to the venue's local time using `pytz`.
2. **Standardization:** The system subtracts **1 day** from the local time because reports received today reflect yesterday's business.
3. **Consistency:** This ensures that reports from Singapore, London, and New York are all dated accurately relative to their own business days, regardless of the UTC offset at the time of the email arrival.

### 🧠 Processing Logic
- **`parser_module`**: The path to the Python parser script (e.g., `src.parsers.malvern_theatre_contractual_report_pdf_parser`).
- **`passthrough_only`**: If `true`, the raw attachment is moved directly to `processed/` for SFTP delivery without modification.
- **`needs_lookup`**: Indicates if the parser requires a local lookup CSV (e.g., for mapping seat categories).

---

## 3. Dynamic Backfilling & Retries

The orchestrator leverages server-side tags for state management:
1. **Rolling Window:** By default, the system scans for untagged emails received within the last **30 days**.
2. **Stateless Logic:** Successful runs apply the `"sales_report_extracted"` tag.
3. **Failure Isolation:** Errors apply the `"sales_report_failed"` tag.
4. **Custom Runs:** Use `days_back`, `target_rule_name`, and `retry_failed` parameters in the Prefect UI for historical corrections.
