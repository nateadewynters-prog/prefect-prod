# 🏛️ Central Orchestration Server (Prefect 3.0)

**Host:** `dew-insights01`  
**OS:** Linux/Ubuntu  
**Python Version:** `3.13.5` (`/usr/bin/python3`)  
**Service Manager:** Docker Compose  
**Virtual Environment:** `/opt/prefect/prod/venv`  

---

## 1. System Architecture

This server hosts multiple automated data pipelines orchestrated by **Prefect 3.0**.

To ensure **independent microservice isolation** and avoid tight coupling, each project maintains its own isolated context:

- They share a centralized configuration file (`.env`) via volume mounts.
- Python utilities (`utils.py`) and dependencies (`requirements.txt`) are localized directly inside each specific project directory, entirely replacing the legacy `shared_libs` directory.
- A standardized `ValidationResult` Data Contract for ETL observability is maintained within each domain's localized `utils.py`.
- **Orchestration of the Brandwatch and Outlook services is managed via `docker-compose.yml`**.

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

## 3. Docker Compose Orchestration

All flows run continuously or on defined schedules using **Docker Compose**. The central `docker-compose.yml` file defines the `prefect-server`, `outlook-automation`, and `brandwatch-sync` services.

### Active Services

| Service Name                   | Directory                  | Target Script / Command             |
|--------------------------------|----------------------------|------------------------------------|
| `prefect-server`              | `/opt/prefect/prod/code/`  | `prefect server start`             |
| `brandwatch-sync`             | `.../brandwatch`           | `sh -c "python brandwatch/... & wait"` |
| `outlook-automation`          | `.../outlook_automation`   | `python -m outlook_automation.email_extraction_flow` |

---

## 4. Global Administration & Troubleshooting

---

### Restarting Services

If you update:

- Any `.py` script  
- The `.env` file  
- Any `.json` configuration  
- The `docker-compose.yml` file

You must rebuild and restart the respective service so changes load into memory.

```bash
# Restart a specific service
docker-compose up -d --build outlook-automation

# Restart all services
docker-compose up -d --build
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

1. Stop running containers
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

🧠 **Parsers**  
```
/opt/prefect/prod/code/outlook_automation/parsers/readme.md
```

---

## 🧭 Operational Principles

This server is designed around:

- **Independent microservice isolation (prioritized over DRY principles)**
- Centralized configuration via `.env` volume mounts
- Docker Compose service-based orchestration
- Strict idempotency
- Secure credential handling
- Observable ETL patterns
- Clear separation of business logic and dependencies by domain

All automation pipelines deployed on `dew-insights01` must adhere to this standard.