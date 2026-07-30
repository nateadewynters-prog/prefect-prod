# 🔍 DocID Reference Tool

**Status:** Microservice (UI Tool)  
**Framework:** Flask + Tabulator.js  
**Internal Port:** `8002`  
**External Port:** `8003`  

---

## 1. Overview

The **DocID Tool** is a high-performance reference table used by the data operations team to quickly look up `ShowId`, `TheatreId`, and `DocumentTypeId` mappings from the `[dbo].[DocumentsAndVenues]` table in the SQL Server `TicketingDS` database. 

It replaces slow, manual SQL queries with a fast, searchable web interface backed by a shared server-side cache.

### Key Features

- **30-Minute Shared Cache:** Data is fetched from SQL at most once every 30 minutes and held in a small SQLite file shared by all Gunicorn workers.
- **Ultra-Fast Search:** Uses Tabulator's virtual DOM to filter through thousands of records instantly without page reloads.
- **Manual Resync:** Includes a "Resync" button to force an immediate refresh of the SQL data cache.
- **Debounced Input:** Search queries are debounced (300ms) to prevent UI lag during intensive filtering.
- **Degrades rather than fails:** if SQL is unreachable the last good cache is still served, and a failed refresh backs off for 60s instead of retrying on every request.

---

## 2. Technical Stack

- **Backend:** Python 3.11, Flask (served by Gunicorn, 4 workers — the cache is shared between them via SQLite)
- **Database:** `pyodbc` (Microsoft ODBC Driver 18)
- **Frontend:** Tailwind CSS, Tabulator.js (Semantic UI Theme)
- **Deployment:** Docker (Debian Bookworm Slim)

---

## 3. Configuration (.env)

The service requires the following environment variables to be mapped via the central `.env` file:

```env
SQL_SERVER=your_server_address
SQL_USERNAME_BILOGIN=your_username
SQL_PASSWORD_BILOGIN=your_password
```

All three are validated at refresh time; a missing one is reported by name in the log rather than surfacing as an opaque ODBC error.

**Optional:**

```env
DOCID_CACHE_DB=/app/docid_cache.db   # cache location; defaults to alongside app.py
```

> The cache file is **not** currently bind-mounted, so it is lost on redeploy. That is not free — see `ANALYSIS.md` (H8/L5) for why, and for the proposed `./data` mount.

---

## 4. Deployment

The service is managed via the root `docker-compose.yml`.

### Internal vs External Ports
| Environment | Port |
|-------------|------|
| **Container (Internal)** | `8002` |
| **Host (External)** | `8003` |

### Volume Mounts
- `/opt/prefect/prod/.env` -> `/app/.env:ro` (Read-only access to global secrets)

---

## 5. Development & Troubleshooting

### Updating the Cache Logic
The caching logic is in `app.py`: `read_cache()` / `write_cache()` wrap the SQLite file, `update_docid_cache()` does the SQL fetch and dedupe, and `_should_refresh()` owns the TTL and failure-backoff rules. `CACHE_TTL_SECONDS` is the single source of truth for the TTL.

### Health
```bash
curl -s http://localhost:8003/health | python3 -m json.tool
```
Reports row count, cache age and staleness — not just whether the process is alive. The container `HEALTHCHECK` uses this endpoint, because the page at `/` renders fine even when the API is completely broken.

### Tests
```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```
SQL and `pyodbc` are mocked, so no database or ODBC driver is needed.

### Local Rebuild
```bash
docker compose up -d --build docid-tool
```

### Logs
```bash
docker logs -f docid-tool
```

### Known issues
See `ANALYSIS.md` for the outstanding findings, including the ID-coercion and dedupe defects that are pending a decision.
