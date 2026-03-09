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
- **`sender_domain`**: The verified source domain of the email (Validated in Python).
- **`subject_keyword`**: String used for simplified KQL filtering in the Graph API.
- **`attachment_type`**: Strict extension enforcement (e.g., `.pdf`).

### 🏷️ Metadata Mapping (Medallion)
- **`show_name`**, **`venue_name`**: Used for standard filename generation.
- **`show_id`**, **`venue_id`**, **`document_id`**: Identifiers for downstream systems.
- **`timezone`**: The exact IANA Time Zone (e.g., `America/New_York`, `Europe/London`) used to perfectly align the UTC email receipt time with the venue's local reporting date.

### ⚙️ Processing Block
- **`passthrough_only`**: (Boolean) If `true`, skips parsing and moves the raw attachment directly to the `processed/` folder for SFTP upload.
- **`parser_module`**: Python module path (e.g., `src.parsers.malvern_theatre_parser`). Required if `passthrough_only` is false.
- **`parser_function`**: The specific entrypoint function name. Required if `passthrough_only` is false.
- **`needs_lookup`**: (Boolean) Toggle for event-date enrichment via `data/lookups/`. Only applicable when `passthrough_only` is false.

---

## 3. Dynamic Backfilling

The orchestrator has moved away from local JSON-based state tracking (`backfill_since`).
1. **Rolling Window:** By default, the system scans for untagged emails received within the last **30 days**.
2. **Stateless Logic:** Once an email is successfully processed, it is tagged as `"sales_report_extracted"` on the Exchange server, ensuring it is ignored in subsequent runs.
3. **Custom Runs:** Historical backfills beyond 30 days can be triggered via the Prefect UI using the `days_back` parameter.

---

## 4. Global Settings

The `global_settings` block defines the relative paths to the Medallion zones (`inbox`, `processed`, `archive`, `failed`, `lookups`).
