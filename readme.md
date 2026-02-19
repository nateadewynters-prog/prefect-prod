# 🏛️ Central Orchestration Server (Prefect 3.0)

**Host:** `DEW-DBSYNC01` (Internal IP: `10.1.50.126`)  
**OS:** Windows Server 10.0.14393  
**Python Version:** 3.13.5 (`C:\Program Files\Python313`)  
**Service Manager:** NSSM (`C:\Users\batchuser\nssm-2.24-101-g897c7ad\win64\nssm.exe`)  
**Virtual Environment:** `C:\Prefect\venv`  

---

## 1. System Architecture

This server hosts multiple automated data pipelines orchestrated by Prefect 3.0.

To maintain **DRY principles**, all projects share:

- A centralized configuration file (`.env`)
- A unified Python utility library (`shared_lib`)

---

### Directory Tree

```text
C:\Prefect\
├── README.md                     <-- Master System Documentation (This File)
├── .env                          <-- Centralized Secrets & API Keys
├── shared_lib\                   <-- Unified Python Utilities
│   └── utils.py                  # Handles SQL, .env loading, and Teams Webhooks
├── brandwatch\                   
│   ├── README.md                 <-- Brandwatch Project Documentation
│   ├── brandwatch_channel_sync.py
│   ├── brandwatch_comments_sync.py
│   └── brandwatch_content_sync.py
└── outlook_automation\           
    ├── README.md                 <-- Email Extraction Project Documentation
    ├── email_extraction_flow.py
    ├── config\                   # JSON routing rules
    └── parsers\                  # PDF/Excel extraction logic
```

---

## 2. Global Configuration & Security

All environment variables, database credentials, and API keys are stored in:

```
C:\Prefect\.env
```

⚠️ **Never hardcode credentials inside scripts.**

All scripts must load configuration via the shared utility:

```python
import sys
from pathlib import Path

# Add C:\Prefect to Python path
sys.path.append(str(Path(__file__).parents[1]))

import shared_lib.utils as utils

utils.setup_environment()
```

This guarantees:

- Centralized credential management  
- Secure configuration  
- No duplicated `.env` logic  
- Simplified maintenance  

---

## 3. Windows Services (NSSM)

All flows run continuously or on defined schedules using **Windows Services managed by NSSM**.

### ⚠️ Important PowerShell Note

All NSSM commands must reference the exact executable path:

```powershell
& "C:\Users\batchuser\nssm-2.24-101-g897c7ad\win64\nssm.exe" <command>
```

---

### Active Services

| Service Name | Directory | Target Script | Schedule |
|--------------|-----------|--------------|----------|
| prefect-server | `C:\Prefect\` | `prefect server start` | Continuous (Port 4200) |
| brandwatch-channel-service | `...\brandwatch` | `brandwatch_channel_sync.py` | Daily @ 07:00 AM |
| brandwatch-content-service | `...\brandwatch` | `brandwatch_content_sync.py` | Daily @ 08:00 AM |
| brandwatch-comments-service | `...\brandwatch` | `brandwatch_comments_sync.py` | Daily @ 09:30 AM |
| outlook-automation-service | `...\outlook_automation` | `email_extraction_flow.py` | Continuous (15m Polling) |

---

## 4. Global Administration & Troubleshooting

### Restarting Services

If you update:

- Any `.py` script  
- The `.env` file  
- Any `.json` configuration  

You **must restart** the respective service so changes load into memory.

```powershell
# Restart a specific service
Restart-Service -Name "outlook-automation-service"

# Restart all Brandwatch services
Restart-Service -Name "brandwatch-*-service"
```

---

### Dashboard Access & Firewall

Prefect UI:

```
http://10.1.50.126:4200
```

If inaccessible from another machine, verify firewall rule:

```powershell
New-NetFirewallRule -DisplayName "Allow Prefect UI" `
    -Direction Inbound `
    -LocalPort 4200 `
    -Protocol TCP `
    -Action Allow
```

---

### Version Control (Portable Git)

Repository is managed using **Portable Git**.

**Executable:**

```
C:\Users\batchuser\PortableGit\bin\git.exe
```

---

#### Standard Workflow

Open PowerShell in:

```
C:\Prefect
```

Stage changes:

```powershell
& "C:\Users\batchuser\PortableGit\bin\git.exe" add .
```

Commit:

```powershell
& "C:\Users\batchuser\PortableGit\bin\git.exe" commit -m "Update message"
```

---

#### Git Lock Errors

If Git fails with:

```
fatal: unable to write new index file
```

This typically means a background service locked the file.

**Resolution Steps:**

1. Stop Prefect services  
2. Delete lock file:

```powershell
del .git\index.lock
```

3. Re-run Git command as Administrator  

---

## 5. Project-Specific Documentation

For detailed business logic, API routing, database schema, or project-specific troubleshooting, refer to:
W
📊 **Brandwatch Sync**  
```
C:\Prefect\brandwatch\README.md
```

📧 **Outlook Extraction**  
```
C:\Prefect\outlook_automation\README.md
```

---

## 🧩 System Summary

This orchestration server provides:

- Centralized credential management  
- Unified shared utilities  
- Prefect 3.0 orchestration  
- Windows-native service automation  
- Secure API integrations  
- Continuous monitoring & observability  