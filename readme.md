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
├── homepage/
│   ├── Dockerfile                <-- Nginx Build Context
│   └── index.html                <-- Service Routing Hub
├── docid_tool/
│   ├── Dockerfile                <-- Flask + SQL Build Context
│   ├── app.py                    <-- Cached SQL Table Engine
│   └── templates/                # 🎨 Frontend (Tabulator.js)
├── powerbi_sales_report_dispatcher/
│   ├── Dockerfile                <-- Flask + MSAL Build Context
│   ├── app.py                    <-- PBI/Graph Dispatch Logic
│   └── templates/                # 🎨 Frontend (SSE Terminal)
├── sales_report_extraction/           
│   ├── readme.md                 <-- Email Extraction Project Documentation
│   ├── Dockerfile.sales          <-- Isolated Build Context
│   └── ...
└── brandwatch_extraction/           
    ├── readme.md                 <-- Social Extraction Project Documentation
    ├── Dockerfile.brandwatch     <-- Isolated Build Context
    └── ...
```

---

## 2. Global Configuration & Security

All environment variables, database credentials, and API keys are stored in:

```
/opt/prefect/prod/.env
```

⚠️ **Never hardcode credentials inside scripts.** All services load configuration via localized environment loaders.

---

## 3. Docker Compose Orchestration

All flows and UI tools run continuously using **Docker Compose**. 

### Active Services

| Service Name                   | Port (Ext) | Directory                       | Function                          |
|--------------------------------|------------|---------------------------------|-----------------------------------|
| `homepage`                     | `80`       | `/homepage`                     | Lightweight Service Hub (Nginx)   |
| `docid-tool`                   | `8003`     | `/docid_tool`                   | SQL DocID Reference (Flask)       |
| `powerbi-dispatcher`           | `8002`     | `/powerbi_sales_report_dispatcher` | PBI Refresher & Emailer (Flask)  |
| `prefect-server`               | `4200`     | `/`                             | Prefect 3.0 Orchestration Brain   |
| `portainer`                    | `9000`     | `n/a`                           | Container Management GUI          |
| `sales-report-extraction`      | `n/a`      | `/sales_report_extraction`      | Background Email ETL (Prefect)    |
| `brandwatch-extraction`        | `n/a`      | `/brandwatch_extraction`        | Background Social ETL (Prefect)   |

---

## 4. Global Administration & Troubleshooting

### Restarting Services

Rebuild and restart services after updating scripts, `.env`, or configurations:

```bash
# Restart a specific service
docker-compose up -d --build powerbi-sales-report-dispatcher
docker-compose up -d --build docid-tool
docker-compose up -d --build homepage

# Restart all services
docker-compose up -d --build
```

### Dashboard Access

- **Central Hub:** `http://dew-insights01/` (Port 80)
- **Prefect UI:** `http://dew-insights01:4200`
- **Portainer:** `http://dew-insights01:9000`

---

## 5. Project-Specific Documentation

🔍 **DocID Reference Tool**  
`.../docid_tool/README.md`

📊 **Power BI Dispatcher**  
`.../powerbi_sales_report_dispatcher/README.md`

🏠 **Service Homepage**  
`.../homepage/README.md`

📧 **Outlook Extraction (Sales Reports)**  
`.../sales_report_extraction/readme.md`

📊 **Brandwatch Social Performance**  
`.../brandwatch_extraction/readme.md`
