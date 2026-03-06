# 🛠️ Application Source (src)

**Domain:** Core Business Logic & Infrastructure  
**Structure:** Modular Service Pattern  

---

## 1. Overview

This directory contains the application logic, decoupled from the orchestration layer.

---

## 2. Component Layout

### 📡 API & External Clients
- **`graph_client.py`**: Specialized client for Microsoft Graph API. Handles OIDC/MSAL authentication, fuzzy searching, attachment downloading, and **category tagging** for state management. Features HTTP 409/412 retry logic for robust tagging.
- **`sftp_client.py`**: A `paramiko`-based client for delivering processed CSVs or raw passthrough files to the Sales Database.

### 🧠 Core Engine
- **`file_processor.py`**: The `ProcessingEngine` class. Manages file lifecycles (renaming, moving across medallion zones) and dynamic parser invocation. Handles the `"passthrough_only"` logic for raw file routing.

### 🧱 Shared Models & Utilities
- **`models.py`**: Data contracts (e.g., `ValidationResult`).
- **`database.py`**: Shared pyodbc logic.
- **`env_setup.py`**: Centralized environment loader.
- **`notifications.py`**: MS Teams adaptive card logic.

---

## 3. Design Principles

1. **Microservice Isolation:** Logic is split into specialized modules to prevent cross-domain regressions.
2. **Server-Side State:** The pipeline relies on external API state (Graph Tags) for idempotency, ensuring the system remains stateless locally.
3. **Data Integrity:** Employs `f.flush()` and `os.fsync()` before SFTP uploads to ensure complete file writes.
4. **Absolute Imports:** All internal imports use the `src.` prefix (e.g., `from src.models import ...`).
