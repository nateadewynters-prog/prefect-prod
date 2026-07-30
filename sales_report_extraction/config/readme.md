# ⚙️ Configuration & Routing Rules

**Domain:** Orchestration & Data Mapping  
**Format:** SharePoint List Rules + JSON Global Settings  

---

## 1. Overview

This directory contains the central configuration layer. All email routing, medallion mapping, and parser assignments are driven by a live SharePoint List of rules, loaded fresh at the start of every flow run by `SharePointRuleLoader` (`src/config_loader.py`) — there is no caching and no local fallback. The `show_reporting_rules.json` file in this directory no longer holds any rules; it now holds only the global settings shared by every run.

---

## 2. Rule Structure (SharePoint List)

Each row in the SharePoint List controls one unique data flow. Rows are pulled live via Microsoft Graph and reshaped into the same nested rule dict (`match_criteria` / `metadata` / `processing`) the rest of the pipeline already understands.

The list's display name is read from the `SHAREPOINT_RULES_LIST_NAME` environment variable (defaults to `Sales Reporting - Master List - Automation - Python`). Each rule is auto-named as `{Show Name}_{VenueName}_{ReportType}`, uppercased with spaces replaced by underscores and empty parts dropped (e.g. `JESUS_CHRIST_SUPERSTAR_MANILA_ADVANCE`) — this `rule_name` is the value used for the flow's `target_rule_name` parameter. Including the report type lets one show and venue carry more than one rule (e.g. an Advance and a Cumulative report) without the names colliding. Rows missing a show name (`Title`) or `VenueName` are skipped with a warning, and inactive rows (`Active` unticked or blank) are still loaded but skipped by the pipeline. After each successful extraction the pipeline stamps that row's `LastRun` column with the UK time of the run. The live matrix of active shows and venues is managed directly in the SharePoint List, not duplicated here.

### 📡 Match Criteria
- **`SenderDomain`**: The verified source domain of the email.
- **`SubjectKeyword`**: String used for simplified, robust subject-only keyword search in the Graph API.
- **`AttachmentType`**: Strict extension enforcement (e.g., `.pdf`, `.xls`).
- **`AttachmentSource`**: Optional. Left blank for a standard physical email attachment (the pipeline defaults to `physical`); set to `html_link` for link-based reports.
- **`FileNameKeyword`**: Optional. When one email carries attachments for several venues, this fragment picks the right one by file name (e.g. `MRSH` vs `MRGZ`). Left blank, the first attachment matching `AttachmentType` wins.

### 🏷️ Metadata Mapping (Medallion)
- **`Title`** (shown as "Show Name" in the SharePoint UI), **`VenueName`**, **`ReportType`**: Used for standard filename generation. `ReportType` is required — filename generation raises a `KeyError` without it.
- **`ShowID`**, **`VenueID`**, **`DocumentID`**: Identifiers for downstream systems. Values are coerced to clean ID strings (trailing `.0` from Number columns is stripped).
- **`Timezone`**: The exact IANA Time Zone (e.g., `Asia/Singapore`, `Europe/London`). Defaults to `UTC` if left blank.

### 🌐 Deterministic Timezone Logic & Sales Day Offset
The engine uses the `Timezone` and optional `SalesDayOffsetHours` columns to perfectly align the UTC email receipt time with the venue's local reporting date:
1. **Sales Day Offset:** For venues with late-night performances, `SalesDayOffsetHours` (e.g., `2`) is added to the arrival time. This pushes emails received in the early morning hours (e.g., 1 AM) into the "effective" next day before the date calculation. Defaults to `0` if left blank.
2. **Local Conversion:** The (offset) UTC time is converted to the venue's local time (e.g., `Asia/Singapore`) using `pytz`.
3. **Standardization:** The system subtracts **1 day** from the local time because reports received today reflect yesterday's business.
4. **Consistency:** This ensures that reports from Singapore, London, and New York are all dated accurately relative to their own business days, regardless of the UTC offset or late-night arrival.

### 🧠 Processing Logic
- **`ParserModule`** / **`ParserFunction`**: When both are filled in, the rule is a parser rule using the named Python parser (e.g., module `src.parsers.malvern_theatre_contractual_report_pdf_parser`). When both are left blank, the rule is passthrough: the raw attachment is moved directly to `archive/` and delivered from there to SFTP without modification (passthrough files are not pushed to the SharePoint `Processed` folder). If only one of the two is filled in, the row is skipped entirely with a warning.
- **`NeedsLookup`**: Indicates if the parser requires a local lookup CSV (e.g., for mapping seat categories). Only meaningful for parser rules.

### 🗂️ Global Settings (`show_reporting_rules.json`)
The JSON file in this directory no longer carries any rules — it now holds only the `global_settings` shared by every run:
- **`base_dir`**: The absolute path to the project root.
- **`data_dirs`**: Relative paths for the `inbox`, `processed`, `archive`, `failed`, and `lookups` folders.

---

## 3. Dynamic Backfilling & Idempotency

The orchestrator leverages server-side tags and client-side fingerprinting for state management:
1. **Rolling Window:** By default, the system scans for untagged emails received within the `days_back` window (default **7 days**).
2. **Fingerprint Deduplication:** The unique `internetMessageId` is used to detect duplicate emails (twins) within the window, tracked per rule. Duplicates are tagged as `"sales_report_duplicate"` and skipped; two different rules can still each claim the same email (see `FileNameKeyword`).
3. **Stateless Logic:** Successful runs apply the `"sales_report_extracted"` tag.
4. **Failure Isolation:** Errors apply the `"sales_report_failed"` tag.
5. **Custom Runs:** Use `days_back`, `target_rule_name`, and `retry_failed` parameters in the Prefect UI for historical corrections.
