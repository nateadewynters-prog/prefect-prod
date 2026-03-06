# 🏛️ Central Orchestration Server (Prefect 3.0)

**Host:** `dew-insights01`  
**OS:** Linux/Ubuntu  
**Python Version:** `3.13.5` (`/usr/bin/python3`)  
**Service Manager:** Docker Compose  
**Virtual Environment:** `/opt/prefect/prod/venv`  

---

## 1. System Architecture

This server hosts automated data pipelines orchestrated by **Prefect 3.0**.

To ensure **independent microservice isolation** and avoid tight coupling, each project maintains its own isolated context:

- They share a centralized configuration file (`.env`) via volume mounts.
- Infrastructure (Docker, requirements) is separated from application code.
- Utility functions are strictly modularized (e.g., inside `src/`) rather than existing in a monolithic `utils.py`.
- A standardized `ValidationResult` Data Contract for ETL observability is maintained within the domain's core models.
- **Orchestration of the Sales Report Extraction service is managed via `docker-compose.yml`**.

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
└── sales_report_extraction/           
    ├── readme.md                 <-- Email Extraction Project Documentation
    ├── Dockerfile.sales          <-- Isolated Build Context
    ├── requirements.txt          <-- Local Python Dependencies
    ├── main.py                   <-- Main Prefect Entrypoint
    ├── config/                   # JSON routing rules
    ├── data/                     # Stateful Medallion data (inbox, processed, etc.)
    ├── src/                      # 🛠️ Application Source
    │   ├── graph_client.py       # API Clients (Graph API, etc.)
    │   ├── file_processor.py     # Business logic & File processing
    │   ├── models.py             # Data Contracts
    │   ├── database.py           # Shared internal utilities (DB)
    │   ├── env_setup.py          # Shared internal utilities (Env)
    │   ├── notifications.py      # Shared internal utilities (Notifications)
    │   ├── sftp_client.py        # SFTP delivery client
    │   └── parsers/              # Vendor-specific PDF/Excel parsers
    └── tests/                    # Isolated test suite
```

---

## 2. Global Configuration & Security

All environment variables, database credentials, and API keys are stored in:

```
/opt/prefect/prod/.env
```

⚠️ **Never hardcode credentials inside scripts.**

All services must load configuration via their localized environment setup utilities.

---

## 3. Docker Compose Orchestration

All flows run continuously or on defined schedules using **Docker Compose**. The central `docker-compose.yml` file defines the `prefect-server` and `sales-report-extraction` services.

### Active Services

| Service Name                   | Directory                  | Target Script / Command             |
|--------------------------------|----------------------------|------------------------------------|
| `prefect-server`              | `/opt/prefect/prod/code/`  | `prefect server start`             |
| `sales-report-extraction`     | `.../sales_report_extraction` | `python main.py`                   |

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
docker-compose up -d --build sales-report-extraction

# Restart all services
docker-compose up -d --build
```

---

### Dashboard Access & Firewall

Prefect UI:

```
http://dew-insights01:4200
```

---

### Version Control (Git)

Repository is managed using standard **Git**.

---

## 5. Project-Specific Documentation

For detailed business logic, API routing, database schema, or project-specific troubleshooting, refer to:

📧 **Outlook Extraction**  
```
/opt/prefect/prod/code/sales_report_extraction/readme.md
```

🧠 **Parsers**  
```
/opt/prefect/prod/code/sales_report_extraction/src/parsers/readme.md
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
