# 💾 Medallion Storage (data)

**Domain:** Data Persistence & State Management  
**Architecture:** Medallion Data Pattern  

> **System Map Reference:** For orchestrator instructions and data flow details, refer to the **Sales Extraction README** at `/opt/prefect/prod/code/sales_report_extraction/readme.md`.

---

## 1. Overview

This directory serves as the stateful storage layer for the Sales Extraction pipeline. It is organized using the **Medallion Architecture**, ensuring clear separation between raw files, processed data, and failed transactions.

---

## 2. Medallion Zones

### 📥 1. Inbox (Landing Zone)
Temporary landing zone for raw attachments downloaded via the Graph API. Files are renamed using a standardized format before processing.

### ✅ 2. Processed (Curated CSVs)
The final extraction output. Parsed data is saved here as standardized CSVs. These files are typically uploaded to the legacy Sales Database via SFTP.

### 📦 3. Archive (Historical Raw)
Raw files (PDF/XLS) are moved here after successful extraction to prevent redundant processing.

### ⚠️ 4. Failed (Quarantine)
If a parser validation or delivery fails, the raw file is moved here for manual investigation.

### 🔍 5. Lookups (Enrichment)
Stateful CSVs containing event-date mappings (e.g., `show_id_venue_id_event_dates.csv`). Used to enrich reports that lack native performance timestamps.

---

## 3. Audit & Progress Tracking

- **`processed_ids.txt`**: A stateful audit log that tracks `ISO Timestamp, Message ID, Rule Name`. The `ProcessingEngine` checks this file to skip duplicate emails.

---

## 4. Git Policy (Security)

**CRITICAL:** Under NO circumstances should raw data files, processed CSVs, or lookups be committed to Git.
1. The `.gitignore` file enforces a blanket ignore on these directories.
2. Only the `.gitkeep` files are tracked to maintain the directory structure.
3. If you add a new data sub-directory, ensure it is added to the ignore list.
