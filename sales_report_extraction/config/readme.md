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
- **`subject_keyword`**: String used to filter target reports.
- **`attachment_type`**: Strict extension enforcement (e.g., `.pdf`).

### 🏷️ Metadata Mapping (Medallion)
- **`show_name`**, **`venue_name`**: Used for standard filename generation.
- **`show_id`**, **`venue_id`**, **`document_id`**: Identifiers for downstream systems.

### ⚙️ Processing Block
- **`passthrough_only`**: (Boolean) If `true`, skips parsing and moves the raw attachment directly to the `processed/` folder for SFTP upload.
- **`parser_module`**: Python module path (e.g., `src.parsers.malvern_theatre_parser`). Required if `passthrough_only` is false.
- **`parser_function`**: The specific entrypoint function name. Required if `passthrough_only` is false.
- **`needs_lookup`**: (Boolean) Toggle for event-date enrichment via `data/lookups/`. Only applicable when `passthrough_only` is false.

---

## 3. Stateful Backfilling

The orchestrator uses the **`backfill_since`** field (YYYY-MM-DD) as a temporal search boundary.
1. The system searches for emails received AFTER this date.
2. After successful extraction, this date is updated to the latest received timestamp.
3. **Note:** Email-level idempotency is handled by the `"sales_report_extracted"` category tag on the Exchange server, ensuring that the system remains stateless locally.

---

## 4. Global Settings

The `global_settings` block defines the relative paths to the Medallion zones (inbox, processed, etc.), allowing the `ProcessingEngine` to remain portable across environments.
