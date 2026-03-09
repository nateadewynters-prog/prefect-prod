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
- **`subject_keyword`**: String used for simplified KQL filtering in the Graph API.
- **`attachment_type`**: Strict extension enforcement (e.g., `.pdf`).

### 🏷️ Metadata Mapping (Medallion)
- **`show_name`**, **`venue_name`**: Used for standard filename generation.
- **`show_id`**, **`venue_id`**, **`document_id`**: Identifiers for downstream systems.
- **`timezone`**: The exact IANA Time Zone (e.g., `Asia/Singapore`).

### 🌐 Deterministic Timezone Logic
The engine uses the `timezone` field to perfectly align the UTC email receipt time with the venue's local reporting date:
1. **Conversion:** UTC (from MS Graph) is converted to the venue's local time using `pytz`.
2. **Standardization:** The system subtracts **1 day** from the local time because reports received today reflect yesterday's business.
3. **Consistency:** This ensures that reports from Singapore, London, and New York are all dated accurately relative to their own business days, regardless of the UTC offset at the time of the email arrival.

---

## 3. Dynamic Backfilling

The orchestrator has moved away from local JSON-based state tracking.
1. **Rolling Window:** By default, the system scans for untagged emails received within the last **30 days**.
2. **Stateless Logic:** Successful runs apply the `"sales_report_extracted"` tag to the email on the server.
3. **Custom Runs:** Historical backfills can be triggered via the Prefect UI using the `days_back` and `target_rule_name` parameters.
