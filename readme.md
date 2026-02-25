# 🏛️ Central Orchestration Server (Prefect 3.0)

**Host:** `dew-insights01`  
**OS:** Linux/Ubuntu  
**Python Version:** `3.13.5` (`/usr/bin/python3`)  
**Service Manager:** systemd (`/etc/systemd/system/`)  
**Virtual Environment:** `/opt/prefect/prod/venv`  

---

## 1. System Architecture

This server hosts multiple automated data pipelines orchestrated by **Prefect 3.0**.

To ensure **independent microservice isolation** and avoid tight coupling, each project maintains its own isolated context:

- They share a centralized configuration file (`.env`) via volume mounts.
- Python utilities (`utils.py`) and dependencies (`requirements.txt`) are localized directly inside each specific project directory, entirely replacing the legacy `shared_libs` directory.
- A standardized `ValidationResult` Data Contract for ETL observability is maintained within each domain's localized `utils.py`.

This architectural pattern prioritizes decoupled microservices over strict DRY principles, ensuring:

- True service isolation  
- Independent dependency management per domain  
- Self-contained Docker build contexts  
- Localized utility evolution without cross-domain regressions  

---

## Directory Tree

```text
/opt/prefect/prod/code/
├── readme.md                     <-- Master System Documentation (This File)
├── docker-compose.yml            <-- Orchestration configuration
├── .env                          <-- Centralized Secrets & API Keys
├── brandwatch/                   
│   ├── readme.md                 <-- Brandwatch Project Documentation
│   ├── Dockerfile.brandwatch     <-- Isolated Build Context
│   ├── requirements.txt          <-- Local Python Dependencies
│   ├── utils.py                  # Handles SQL, .env loading, Teams Webhooks, and Data Contracts
│   ├── brandwatch_channel_sync.py
│   ├── brandwatch_comments_sync.py
│   └── brandwatch_content_sync.py
└── outlook_automation/           
    ├── readme.md                 <-- Email Extraction Project Documentation
    ├── Dockerfile.outlook        <-- Isolated Build Context
    ├── requirements.txt          <-- Local Python Dependencies
    ├── utils.py                  # Handles SQL, .env loading, Teams Webhooks, and Data Contracts
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

All scripts must load configuration via their localized utility:

```python
import utils

utils.setup_environment()
```

This guarantees:

- Centralized credential management  
- Secure configuration loading  
- True microservice independence without fragile `sys.path.append` logic  
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

- **Independent microservice isolation (prioritized over DRY principles)**
- Centralized configuration via `.env` volume mounts
- Service-based orchestration
- Strict idempotency
- Secure credential handling
- Observable ETL patterns
- Clear separation of business logic and dependencies by domain

All automation pipelines deployed on `dew-insights01` must adhere to this standard.
