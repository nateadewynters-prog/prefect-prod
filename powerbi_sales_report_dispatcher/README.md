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
- **Global Lock:** Only one report can be dispatched at a time; requests for other shows are rejected while a dispatch is in progress.
- **Dispatch History:** Run timings and PDF sizes are persisted to a SQLite state database (`locks`, `logs`, `dispatch_history` tables) and surfaced in the UI.
- **Hybrid Data:** Combines the visual depth of Power BI exports with the raw precision of direct SQL metrics in the email body.
- **Auth Integration:** Uses `msal` for secure, client-secret authentication with Azure AD.

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
BUSINESS_INTELLIGENCE_INBOX_ADDRESS=sender_mailbox_address

# Database Access
SQL_SERVER=your_server_address
SQL_USERNAME_BILOGIN=your_username
SQL_PASSWORD_BILOGIN=your_password
```

### Show Configuration
Individual show details (Workspace IDs, Report IDs, etc.) are defined in the `SHOWS_CONFIG` list within `config.py`.

---

## 4. Module Layout

The app is split by responsibility, mirroring `powerbi_media_report_dispatcher`
so both tools read the same way.

| File | Holds |
|------|-------|
| `config.py` | Settings, `SHOWS_CONFIG`, `STAGES` — **the file you normally edit** |
| `state.py` | SQLite: locks, logs, dispatch history (and the column migrations) |
| `services/auth.py` | Azure AD tokens (Power BI + Graph scopes) |
| `services/sql.py` | Sales metrics from SQL Server (`Legacy` / `TransactLive` router) |
| `services/powerbi.py` | Dataset refresh, report export, PDF→PNG preview |
| `services/email.py` | Email HTML builder + MS Graph send |
| `pipeline.py` | The dispatch job and the shared SSE/lock plumbing |
| `routes.py` | HTTP endpoints, as a Blueprint |
| `app.py` | Thin entrypoint — builds the app, inits the DB, registers routes |

### Where does X live?
- *Change a recipient or add a show* → `config.py`
- *Change a log message or the order of pipeline steps* → `pipeline.py`
- *Change the email wording* → `services/email.py`
- *Add an endpoint* → `routes.py`

Two things are a silent contract with `templates/dispatcher.html`: the route
URLs (`/api/state`, `/api/history`, `/query/<id>`, `/preview/<id>`,
`/stream/<id>`) and the stage ids in `config.STAGES`. Changing either without
updating the template breaks the UI without raising an error.

The polling loops for the dataset refresh and the report export deliberately
stay in `pipeline.py` rather than moving into `services/powerbi.py`: each poll
yields a `⏳ ... Status:` line to the live log, and only a generator can do that.

---

## 5. Deployment

The service is managed via the root `docker-compose.yml`.

### Internal vs External Ports
| Environment | Port |
|-------------|------|
| **Container (Internal)** | `8002` |
| **Host (External)** | `8002` |

### Volume Mounts
- `/opt/prefect/prod/.env` -> `/app/.env:ro` (Read-only access to global secrets)
- `./data` -> `/app/data` (Persists the SQLite state database; `DB_PATH=/app/data/dispatcher_state.db` is set in the compose `environment` block)

---

## 6. Development & Troubleshooting

### Adding New Shows
To add a new show to the dispatcher:
1. Locate `SHOWS_CONFIG` in `config.py`.
2. Add a new dictionary with the required `id`, `show_name`, `show_id`, `db_type` (`Legacy` or `TransactLive`), `pbi_workspace_id`, `pbi_report_id`, `pbi_dataset_id`, `dashboard_url` and `recipients`.
3. Rebuild the container.

### Local Rebuild
```bash
docker compose up -d --build powerbi-sales-report-dispatcher
```

### Logs
```bash
docker logs -f powerbi-sales-report-dispatcher
```
