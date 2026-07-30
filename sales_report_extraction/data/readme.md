# 💾 Medallion Storage (data)

**Domain:** Data Persistence & State Management  
**Architecture:** Stateless Medallion Data Pattern  

---

## 1. Overview

This directory provides the storage layer for the pipeline. It follows the **Medallion Architecture**, ensuring clear separation between raw files and processed outputs while maintaining a strictly stateless local profile.

---

## 2. Medallion Zones

### 📥 1. Inbox (Landing Zone)
Temporary landing zone for raw attachments downloaded from the Graph API. Files are standardized using **deterministic timezone logic** (converting UTC to local venue time and incorporating **Sales Day Offset** for late-night reports) before processing.

### ✅ 2. Processed (Curated Parser Output)
The final extraction output for **parser** rules only, written in the format the parser asks for (`.csv`, or whatever its `OUTPUT_EXT` declares). Files here are uploaded to the SharePoint `Processed` folder and delivered to the Sales Database via **SFTP delivery**. Passthrough rules never write here — their unmodified attachment goes to `archive/` and is delivered from there.

### 📦 3. Archive (Historical Raw)
Raw files are moved here after successful extraction for long-term retention. Passthrough rules also land their unmodified attachment here — for those rules, this is the copy delivered to SFTP. 

### ⚠️ 4. Failed (Quarantine)
Files that fail validation, trigger a `ValueError` (mapping/lookup errors), or cause system exceptions are moved here for manual investigation.

### 🔍 5. Lookups
Contains local CSV lookup tables used by parsers (e.g., for mapping vendor-specific category codes to internal IDs).

### 🗃️ 6. Error Tracking
Holds `dataops_tracking.db`, the SQLite store written by `src/error_db_client.py` when a mapping/lookup code cannot be resolved. Unlike the zones above, this folder is not listed in `global_settings.data_dirs` — it is derived by `error_db_client.py` itself.

---

## 3. Stateless Design Note

**This directory is transient and can be safely purged.**

> **Note:** In production this repo folder is *not* the live location. `docker-compose.yml` bind-mounts the host path `/opt/data/outlook_automation/data` over it, so the running container reads and writes there.

Email processing state is managed directly on the Microsoft Exchange server using the **`sales_report_extracted`**, **`sales_report_failed`**, and **`sales_report_duplicate`** category tags. Fingerprint-based deduplication (`internetMessageId`) is also handled by the orchestrator. The pipeline is entirely **stateless locally**, operating on a **dynamic rolling window** (`days_back`, default 7 days). This ensures that even if this `data/` directory is wiped, the pipeline will not process the same email twice unless the tags are manually or programmatically reset.

---

## 4. Git Policy (Security)

**CRITICAL:** Do NOT commit raw data or processed CSVs to Git.
1. The `.gitignore` file excludes the contents of `inbox`, `processed`, `archive`, `failed` and `lookups`. Note it does **not** currently cover `error_tracking/`, and `dataops_tracking.db` is tracked in Git.
2. `.gitkeep` files are used to maintain the directory structure in the repository.
