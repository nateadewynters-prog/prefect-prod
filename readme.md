# 🏛️ Central Orchestration Server (Prefect 3.0)

**Host:** `dew-insights01`  
**OS:** Linux/Ubuntu  
**Python Version:** `3.13.5` (`/usr/bin/python3`)  
**Service Manager:** systemd (`/etc/systemd/system/`)  
**Virtual Environment:** `/opt/prefect/prod/venv`  

---

## 1. System Architecture

This server hosts multiple automated data pipelines orchestrated by **Prefect 3.0**.

To maintain **DRY principles**, all projects share:

- A centralized configuration file (`.env`)
- A unified Python utility library (`shared_libs`)
- A standardized `ValidationResult` Data Contract for ETL observability

This ensures:

- No duplicated credential logic  
- Centralized alerting  
- Consistent database connectivity  
- Unified validation and monitoring patterns  

---

## Directory Tree

```text
/opt/prefect/prod/code/
├── readme.md                     <-- Master System Documentation (This File)
├── requirements.txt              <-- Python Dependencies
├── .env                          <-- Centralized Secrets & API Keys
├── shared_libs/                  <-- Unified Python Utilities
│   └── utils.py                  # Handles SQL, .env loading, Teams Webhooks, and Data Contracts
├── brandwatch/                   
│   ├── readme.md                 <-- Brandwatch Project Documentation
│   ├── brandwatch_channel_sync.py
│   ├── brandwatch_comments_sync.py
│   └── brandwatch_content_sync.py
└── outlook_automation/           
    ├── readme.md                 <-- Email Extraction Project Documentation
    ├── email_extraction_flow.py
    ├── config/                   # JSON routing rules
    └── parsers/                  # PDF/Excel extraction logic
```

---

## 2. Global Configuration & Security

All environment variables, database credentials, and API keys are stored in:

```
/opt/prefect/prod/.env
```

⚠️ **Never hardcode credentials inside scripts.**

All scripts must load configuration via the shared utility:

```python
import sys
from pathlib import Path

# Add /opt/prefect/prod/code to Python path
sys.path.append(str(Path(__file__).parents[1]))

import shared_libs.utils as utils

utils.setup_environment()
```

This guarantees:

- Centralized credential management  
- Secure configuration loading  
- No duplicated `.env` logic  
- Simplified long-term maintenance  

---

## 3. Linux Services (systemd)

All flows run continuously or on defined schedules using **systemd services**.

---

### ⚠️ Important Bash Note

All systemd units must reference the exact python executable path within the virtual environment:

```bash
/opt/prefect/prod/venv/bin/python <script_path>
```

---

### Active Services

| Service Name                   | Directory                  | Target Script / Command             | Schedule                    |
|--------------------------------|----------------------------|------------------------------------|-----------------------------|
| `prefect-server`              | `/opt/prefect/prod/code/`  | `prefect server start`             | Continuous (Port 4200)      |
| `brandwatch-channel`          | `.../brandwatch`           | `brandwatch_channel_sync.py`       | Daily @ 07:00 AM            |
| `brandwatch-content`          | `.../brandwatch`           | `brandwatch_content_sync.py`       | Daily @ 08:00 AM            |
| `brandwatch-comments`         | `.../brandwatch`           | `brandwatch_comments_sync.py`      | Daily @ 09:30 AM            |
| `outlook-automation`          | `.../outlook_automation`   | `email_extraction_flow.py`         | Continuous (15m Polling)    |

---

## 4. Global Administration & Troubleshooting

---

### Restarting Services

If you update:

- Any `.py` script  
- The `.env` file  
- Any `.json` configuration  

You must restart the respective service so changes load into memory.

```bash
# Restart a specific service
sudo systemctl restart outlook-automation

# Restart all Brandwatch services
sudo systemctl restart brandwatch-*
```

---

### Dashboard Access & Firewall

Prefect UI:

```
http://dew-insights01:4200
```

If inaccessible from another machine, verify firewall rule:

```bash
sudo ufw allow 4200/tcp
```

---

### Version Control (Git)

Repository is managed using standard **Git**.

**Executable:**

```
/usr/bin/git
```

---

### Standard Workflow

Open Terminal in:

```
/opt/prefect/prod/code
```

Stage changes:

```bash
git add .
```

Commit:

```bash
git commit -m "Update message"
```

---

### Git Lock Errors

If Git fails with:

```
fatal: unable to write new index file
```

This typically means a background process locked the file.

**Resolution Steps:**

1. Stop Prefect services  
2. Delete lock file:

```bash
rm .git/index.lock
```

3. Re-run Git command (use sudo if permissions issue)

---

## 5. Project-Specific Documentation

For detailed business logic, API routing, database schema, or project-specific troubleshooting, refer to:

📊 **Brandwatch Sync**  
```
/opt/prefect/prod/code/brandwatch/readme.md
```

📧 **Outlook Extraction**  
```
/opt/prefect/prod/code/outlook_automation/readme.md
```

---

## 🧭 Operational Principles

This server is designed around:

- Centralized configuration
- Service-based orchestration
- Strict idempotency
- Secure credential handling
- Modular architecture
- Observable ETL patterns
- Clear separation of business logic by domain

All automation pipelines deployed on `dew-insights01` must adhere to this standard.
