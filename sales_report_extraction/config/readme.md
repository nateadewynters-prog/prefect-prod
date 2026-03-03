# ⚙️ Configuration & Routing Rules

**Domain:** Orchestration & Data Mapping  
**Format:** JSON-Driven Medallion Pattern  

> **System Map Reference:** For orchestrator instructions and data flow details, refer to the **Sales Extraction README** at `/opt/prefect/prod/code/sales_report_extraction/readme.md`.

---

## 1. Overview

This directory contains the central configuration layer for the Sales Extraction pipeline. All email routing, medallion mapping, and parser assignments are handled via declarative JSON rules, eliminating the need for code changes when adding new shows or venues.

---

## 2. Rule Structure (`show_reporting_rules.json`)

Each object in the `"rules"` array controls one unique data flow:

### 📡 Match Criteria
- **`sender_domain`**: The verified source of the email.
- **`subject_keyword`**: String used to filter the target show/report.
- **`attachment_type`**: Strict extension enforcement (e.g., `.pdf`, `.xls`).

### 🏷️ Metadata Mapping (Medallion)
- **`show_name`**, **`venue_name`**: Used to generate standard filenames.
- **`show_id`**, **`venue_id`**, **`document_id`**: Internal database identifiers for downstream systems.

### ⚙️ Processing Block
- **`parser_module`**: The Python module path (e.g., `src.parsers.my_parser`).
- **`parser_function`**: The specific entrypoint inside the module.
- **`needs_lookup`**: Boolean toggle for event-date enrichment via `data/lookups/`.

---

## 3. Stateful Backfilling

The orchestrator uses the **`backfill_since`** field (YYYY-MM-DD) to track progress. 
1. The system only processes emails received AFTER this date.
2. Upon successful extraction, the field is automatically advanced to the latest received date.
3. To re-process historical data, manually roll back this date in the JSON file.

---

## 4. Global Settings

The `global_settings` block defines the relative paths to the Medallion zones and the history log (`processed_ids.txt`), ensuring the `ProcessingEngine` remains context-aware across different environments.
