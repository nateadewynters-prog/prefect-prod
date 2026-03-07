# 💾 Medallion Storage (data)

**Domain:** Data Persistence & State Management  
**Architecture:** Medallion Data Pattern  

---

## 1. Overview

This directory serves as the storage layer for the pipeline. It follows the **Medallion Architecture**, ensuring clear separation between raw files and processed outputs.

---

## 2. Medallion Zones

### 📥 1. Inbox (Landing Zone)
Temporary landing zone for raw attachments downloaded from the Graph API. Files are standardized using metadata before processing.

### ✅ 2. Processed (Curated CSVs & Raw Passthrough)
The final extraction output. This directory contains:
- Standardized CSVs generated from successful extraction.
- Raw attachments for rules configured with `"passthrough_only": true`.
All files in this directory are subsequently uploaded to the Sales Database via SFTP.

### 📦 3. Archive (Historical Raw)
Raw files are moved here after successful extraction for long-term retention. **Note:** For `"passthrough_only"` rules, the file is moved directly to the `Processed` zone and not archived separately here.

### ⚠️ 4. Failed (Quarantine)
Files that fail validation or cause processing errors are moved here for manual investigation.

### 🔍 5. Lookups (Enrichment)
Reference CSVs containing event-date mappings (e.g., `show_id_venue_id_event_dates.csv`).

---

## 3. State Management Note

**Local state tracking has been replaced by server-side tagging.** 

Email processing state is now tracked directly on the Microsoft Exchange server using the **`sales_report_extracted`** category tag via the Graph API. The pipeline is entirely stateless locally and operates on a **30-day dynamic rolling window**. This ensures that even if the local data directory is wiped, the pipeline will not process the same email twice.

---

## 4. Git Policy (Security)

**CRITICAL:** Do NOT commit raw data or processed CSVs to Git.
1. The `.gitignore` file excludes all data directories.
2. `.gitkeep` files are used to maintain the directory structure in the repository.
