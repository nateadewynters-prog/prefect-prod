# 💾 Medallion Storage (data)

**Domain:** Data Persistence & State Management  
**Architecture:** Stateless Medallion Data Pattern  

---

## 1. Overview

This directory provides the storage layer for the pipeline. It follows the **Medallion Architecture**, ensuring clear separation between raw files and processed outputs while maintaining a strictly stateless local profile.

---

## 2. Medallion Zones

### 📥 1. Inbox (Landing Zone)
Temporary landing zone for raw attachments downloaded from the Graph API. Files are standardized using **deterministic timezone logic** (converting UTC to local venue time) before processing.

### ✅ 2. Processed (Curated CSVs & Raw Passthrough)
The final extraction output. All files in this directory are subsequently uploaded to the Sales Database via **SFTP delivery**.

### 📦 3. Archive (Historical Raw)
Raw files are moved here after successful extraction for long-term retention. 

### ⚠️ 4. Failed (Quarantine)
Files that fail validation or trigger a `ValueError` are moved here for manual investigation.

---

## 3. Stateless Design Note

**This directory is transient and can be safely purged.**

Email processing state is managed directly on the Microsoft Exchange server using the **`sales_report_extracted`** category tag. The pipeline is entirely **stateless locally**, operating on a **30-day dynamic rolling window**. This ensures that even if this `data/` directory is wiped, the pipeline will not process the same email twice.

---

## 4. Git Policy (Security)

**CRITICAL:** Do NOT commit raw data or processed CSVs to Git.
1. The `.gitignore` file excludes all data directories.
2. `.gitkeep` files are used to maintain the directory structure in the repository.
