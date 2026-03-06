# 📊 Brandwatch Social Performance Extraction (Prefect 3.0)

**Host:** `dew-insights01`  
**Status:** 🟢 Active (Scheduled 1:00 PM Daily)  
**Orchestration:** Prefect 3.0  

> **System Map Reference:** For global server settings, Docker Compose service management, and Python environment details, refer to the **Master README** at `/opt/prefect/prod/code/readme.md`.

---

## 1. Project Overview

This service automates the extraction of social performance data (Posts, Channels, and Comments) from the Brandwatch (Falcon.io) API. It ensures that organic social metrics are synchronized with the central Azure SQL data warehouse for unified reporting.

---

## 2. Directory Structure & Modular Layout

The project follows a strict `src` layout to separate infrastructure from application logic.

```text
/opt/prefect/prod/code/brandwatch_extraction/
├── Dockerfile.brandwatch     # 🐳 Infrastructure: Container definition
├── requirements.txt          # 📦 Infrastructure: Python dependencies
├── main.py                   # 🤖 ORCHESTRATOR: Main Prefect Entrypoint
├── config/                   # ⚙️ CONFIG: Local configuration
├── data/                     # 💾 STORAGE: Temporary data landing
└── src/                      # 🛠️ APPLICATION: Core logic
    ├── api_client.py         # API requests and multi-key rotation
    ├── database.py           # Azure SQL ingestion (ODBC Driver 18)
    ├── env_setup.py          # Shared internal utilities (Env)
    └── constants.py          # Project-specific constants
```

### Module Responsibilities (Brain & Muscles)

- **`main.py`**: The "Brain" of the operation. It manages the Prefect Flow orchestration and handles the 1:00 PM daily cron schedule.
- **`src/api_client.py`**: Handles all communication with the Brandwatch/Falcon.io API, including complex multi-key rotation logic to manage rate limits.
- **`src/database.py`**: Manages Azure SQL ingestion using the ODBC Driver 18. It handles the "Muscles" of moving data into the staging environment.
- **`src/env_setup.py`**: Ensures explicit loading of the shared environment configuration from `/opt/prefect/prod/.env`.

---

## 3. Data Strategy

To maintain data integrity and capture late-arriving engagement metrics, the system employs a two-tiered synchronization strategy:

- **90-Day Post Sweep**: A rolling window that scans the last 90 days of posts to update engagement metrics (likes, shares, etc.) as they mature.
- **2-Day Settled Sync**: A focused sync for channel-level metrics and comments, ensuring that recently settled data is captured with high precision.

---

## 4. Operations

The service is managed via Docker Compose from the root directory.

### Build & Deploy
To rebuild the container after code changes:
```bash
docker-compose up -d --build brandwatch-extraction
```

### Monitoring Logs
To view real-time execution logs:
```bash
docker-compose logs -f brandwatch-extraction
```

---

## 5. Data Contract

The system enforces a strict landing zone schema to ensure compatibility with downstream ETL processes.

- **Landing Zone**: `[organicsocial].[dbo].[stg_bw_raw_json]`
- **Schema**:
    - `SourceEndpoint`: The API endpoint origin (e.g., `/posts`, `/channels`).
    - `RawData`: The raw JSON payload as received from the API.

---

## ✅ Architectural Guarantees

- **Microservice Isolation**: Independent build context and dependencies.
- **Key Rotation**: Resilient API handling to maximize throughput.
- **Centralized Config**: Leverages the shared `/opt/prefect/prod/.env` volume mount.
- **Observability**: Fully integrated with Prefect 3.0 dashboard and logging.
