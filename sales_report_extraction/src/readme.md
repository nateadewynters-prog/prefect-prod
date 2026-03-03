# 🛠️ Application Source (src)

**Domain:** Core Business Logic & Infrastructure  
**Structure:** Modular Service Pattern  

> **System Map Reference:** For orchestrator instructions and data flow details, refer to the **Sales Extraction README** at `/opt/prefect/prod/code/sales_report_extraction/readme.md`.

---

## 1. Overview

This directory contains the primary application logic for the Sales Report Extraction pipeline. It is decoupled from the orchestration layer (`main.py`) and follows a modular pattern where each script has a single responsibility.

---

## 2. Component Layout

### 📡 API & External Clients
- **`graph_client.py`**: A specialized client for Microsoft Graph API. Handles OIDC/MSAL authentication, fuzzy email searching, and base64 attachment extraction.
- **`sftp_client.py`**: A `paramiko`-based client for delivering final CSVs to the legacy Sales Database.

### 🧠 Core Engine
- **`file_processor.py`**: The `ProcessingEngine` class. Manages the lifecycle of a file through the Medallion zones (inbox -> processed/archive). Handles dynamic parser invocation and stateful configuration updates.

### 🧱 Shared Models & Utilities
- **`models.py`**: Global data contracts (e.g., `ValidationResult`).
- **`database.py`**: Shared pyodbc connection logic for internal reporting.
- **`env_setup.py`**: Centralized environment loader that points to the root `.env` file.
- **`notifications.py`**: MS Teams adaptive card notification logic.

---

## 3. Modularization Principles

This directory adheres to **Microservice Isolation**:
1. **No Monolithic Utils:** Logic is split into `database.py`, `notifications.py`, etc., to prevent cross-domain regressions.
2. **Absolute Imports:** All internal imports use the `src.` prefix (e.g., `from src.models import ...`) to ensure compatibility with both the Docker container and local test runners.
3. **Stateless Clients:** Clients like `GraphClient` and `sftp_client` do not hold state between runs, ensuring idempotency.
