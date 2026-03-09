# 🏛️ Central Orchestration Server (Prefect 3.0)

**Host:** `dew-insights01`  
**OS:** Linux/Ubuntu  
**Python Version:** `3.13.5` (`/usr/bin/python3`)  
**Service Manager:** Docker Compose  
**Virtual Environment:** `/opt/prefect/prod/venv`  

---

## 1. System Architecture

This server hosts automated data pipelines orchestrated by **Prefect 3.0**. The architecture is built on **stateless container design**, ensuring that containers can be destroyed and recreated without data loss or state corruption.

### 🧭 Core Architectural Pillars

- **Microservice Isolation:** Each project maintains its own isolated context, Docker build context, and localized utility evolution.
- **Stateless Design:** Services are strictly "API-to-Database" (Brandwatch) or rely on server-side state (Sales Report Extraction via MS Graph Tags), eliminating local disk dependencies.
- **Granular Task Routing:** Orchestration is decomposed into distinct Prefect `@tasks` with specific retry logic (e.g., handling SQL blips or API rate limits) to ensure resilient execution without full pipeline restarts.
- **Deterministic Logic:** Time-sensitive operations (like report dating) use deterministic timezone logic defined in JSON configurations to ensure global consistency.
- **Unified Data Contracts:** All ETL modules adhere to strict `ValidationResult` contracts for observability and automated alerting.

---

## Directory Tree

```text
/opt/prefect/prod/code/
├── readme.md                     <-- Master System Documentation (This File)
├── docker-compose.yml            <-- Orchestration configuration
├── .env                          <-- Centralized Secrets & API Keys
├── sales_report_extraction/           
│   ├── readme.md                 <-- Email Extraction Project Documentation
│   ├── Dockerfile.sales          <-- Isolated Build Context
│   ├── requirements.txt          <-- Local Python Dependencies
│   ├── main.py                   <-- Main Prefect Entrypoint
│   └── ...
└── brandwatch_extraction/           
    ├── readme.md                 <-- Social Extraction Project Documentation
    ├── Dockerfile.brandwatch     <-- Isolated Build Context
    ├── requirements.txt          <-- Local Python Dependencies
    ├── main.py                   <-- Main Prefect Entrypoint
    └── src/                      # 🛠️ Application Source
        ├── api_client.py         # API Clients (Brandwatch/Falcon.io)
        ├── database.py           # Shared internal utilities (DB)
        └── env_setup.py          # Shared internal utilities (Env)
```

---

## 2. Global Configuration & Security

All environment variables, database credentials, and API keys are stored in:

```
/opt/prefect/prod/.env
```

⚠️ **Never hardcode credentials inside scripts.** All services load configuration via localized `src/env_setup.py` utilities.

---

## 3. Docker Compose Orchestration

All flows run continuously or on defined schedules using **Docker Compose**. 

### Active Services

| Service Name                   | Directory                       | Target Script / Command            |
|--------------------------------|---------------------------------|-----------------------------------|
| `prefect-server`               | `/opt/prefect/prod/code/`       | `prefect server start`            |
| `sales-report-extraction`      | `.../sales_report_extraction`   | `python main.py`                  |
| `brandwatch-extraction`        | `.../brandwatch_extraction`     | `python main.py`                  |

---

## 4. Global Administration & Troubleshooting

### Restarting Services

Rebuild and restart services after updating scripts, `.env`, or configurations:

```bash
# Restart a specific service
docker-compose up -d --build sales-report-extraction
docker-compose up -d --build brandwatch-extraction

# Restart all services
docker-compose up -d --build
```

### Dashboard Access

Prefect UI: `http://dew-insights01:4200`

---

## 5. Project-Specific Documentation

📧 **Outlook Extraction (Sales Reports)**  
`.../sales_report_extraction/readme.md`

📊 **Brandwatch Social Performance**  
`.../brandwatch_extraction/readme.md`
