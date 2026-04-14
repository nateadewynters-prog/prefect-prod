# 📊 Power BI Sales Report Dispatcher

**Status:** Microservice (UI Tool)  
**Framework:** Flask + MSAL + MS Graph API  
**Internal Port:** `8002`  
**External Port:** `8002`  

---

## 1. Overview

The **Power BI Sales Report Dispatcher** is an interactive automation tool designed to streamline the morning reporting workflow. It handles the end-to-end process of:

1. **Triggering** Power BI dataset refreshes.
2. **Polling** for dataset refresh completion.
3. **Fetching** real-time SQL metrics for email summaries.
4. **Exporting** high-resolution PDFs from the Power BI REST API.
5. **Rendering** PNG previews for inline email viewing.
6. **Dispatching** reports via the Microsoft Graph API.

### Key Features

- **Live Terminal (SSE):** Provides real-time, terminal-style feedback to the user as each step of the pipeline executes.
- **Batch Processing:** Ability to run all configured reports in sequence with a single click.
- **Hybrid Data:** Combines the visual depth of Power BI exports with the raw precision of direct SQL metrics in the email body.
- **Auth Integration:** Uses `msal` for secure, certificate-based authentication with Azure AD.

---

## 2. Technical Stack

- **Backend:** Python 3.11, Flask
- **Auth:** `msal` (Microsoft Authentication Library)
- **PDF Processing:** `PyMuPDF` (fitz) for PDF-to-Image rendering.
- **APIs:** Power BI REST API, Microsoft Graph API.
- **Frontend:** Tailwind CSS + JavaScript (EventSource for SSE).

---

## 3. Configuration (.env)

The service requires the following environment variables to be mapped via the central `.env` file:

```env
# Azure AD / Microsoft Graph
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret

# Database Access
SQL_SERVER=your_server_address
SQL_USERNAME=your_username
SQL_PASSWORD=your_password
```

### Show Configuration
Individual show details (Workspace IDs, Report IDs, etc.) are currently defined in the `SHOWS_CONFIG` list within `app.py`.

---

## 4. Deployment

The service is managed via the root `docker-compose.yml`.

### Internal vs External Ports
| Environment | Port |
|-------------|------|
| **Container (Internal)** | `8002` |
| **Host (External)** | `8002` |

### Volume Mounts
- `/opt/prefect/prod/.env` -> `/app/.env:ro` (Read-only access to global secrets)

---

## 5. Development & Troubleshooting

### Adding New Shows
To add a new show to the dispatcher:
1. Locate `SHOWS_CONFIG` in `app.py`.
2. Add a new dictionary with the required `pbi_workspace_id`, `pbi_report_id`, and `pbi_dataset_id`.
3. Rebuild the container.

### Local Rebuild
```bash
docker-compose up -d --build powerbi-sales-report-dispatcher
```

### Logs
```bash
docker logs -f powerbi-sales-report-dispatcher
```
