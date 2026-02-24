# GEMINI.md - Context & Instructions

This file documents the `dew-insights01` Prefect orchestration environment. It serves as the primary context for AI agents and developers working in this directory.

## 🌍 Project Overview

This repository hosts the production code for **Prefect 3.0** data pipelines on the **Linux** server `dew-insights01`. The system automates ETL processes for:
1.  **Outlook Automation:** Extracting financial data from emails (PDF/Excel) using a Medallion Architecture.
2.  **Brandwatch Sync:** Synchronizing social media metrics (Channels, Content, Comments) to a SQL Server database.

## 📂 Directory Structure

*   **`/opt/prefect/prod/code/`**: Root directory.
    *   `readme.md`: Master system documentation.
    *   `.env`: Centralized configuration (Secrets, API Keys). **NEVER COMMIT THIS.**
    *   `requirements.txt`: Python dependencies.
    *   **`shared_libs/`**: Shared utility modules (crucial for DRY principles).
        *   `utils.py`: Handles environment setup, SQL connections, Teams notifications, and Data Contracts (`ValidationResult`).
    *   **`outlook_automation/`**: Email extraction pipelines.
        *   `config/show_reporting_rules.json`: "Brain" of the operation. Defines routing rules and state (`backfill_since`).
        *   `parsers/`: Custom logic for different file formats (PDF/Excel).
    *   **`brandwatch/`**: Social media API synchronization scripts.

## 🛠️ Development Conventions

### 1. Environment & Paths
*   **OS:** Ubuntu Linux.
*   **Python:** `/opt/prefect/prod/venv/bin/python`.
*   **Pathing:** Always use absolute Linux paths or `pathlib` for file operations. Avoid Windows backslashes.
*   **Credentials:** **NEVER hardcode secrets.** Load them via `shared_libs.utils.setup_environment()` which reads from `/opt/prefect/prod/.env`.

### 2. Shared Libraries & DRY
*   **`shared_libs.utils`** is the backbone. Always import it:
    ```python
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parents[1])) # Add root to path
    import shared_libs.utils as utils
    
    utils.setup_environment() # Load env vars
    ```
*   Use `utils.get_db_connection()` for SQL access.
*   Use `utils.send_teams_notification()` for alerts.

### 3. Data Contracts & Validation
*   All ETL parsers must return a `ValidationResult` object (defined in `utils.py`).
*   **Status Levels:**
    *   `PASSED`: Silent success.
    *   `UNVALIDATED`: Warning (sends Teams alert, requires manual review).
    *   `FAILED`: Hard stop (sends Critical Teams alert).

### 4. Idempotency & State
*   Pipelines must be re-runnable without side effects.
*   **Outlook:** Uses `processed_ids.txt` (Audit Log) and `backfill_since` (JSON State) to prevent duplicates.
*   **Brandwatch:** Deletes existing records for the target date before inserting new ones.

## 🚀 Operational Commands

### Virtual Environment
```bash
source /opt/prefect/prod/venv/bin/activate
```

### Running Flows (Manual)
```bash
# Example: Run Outlook Automation
/opt/prefect/prod/venv/bin/python /opt/prefect/prod/code/outlook_automation/email_extraction_flow.py
```

### System Services (systemd)
All flows run as systemd services.
```bash
# Check status
sudo systemctl status outlook-automation
sudo systemctl status brandwatch-content

# Restart after code changes
sudo systemctl restart outlook-automation
```

### Git Workflow
*   **Remote:** Portable Git (historically), now standard Linux Git.
*   **Workflow:**
    1.  `git pull`
    2.  Make changes.
    3.  `git add .`
    4.  `git commit -m "Description"`
    5.  **Restart relevant systemd services** to apply changes.

## ⚠️ Critical Constraints
*   **No Windows Paths:** Ensure all code uses `/` forward slashes.
*   **Dependency Management:** If adding libraries, update `requirements.txt` and rebuild the venv if necessary.
*   **Security:** Verify `.gitignore` excludes `.env` and `__pycache__`.
