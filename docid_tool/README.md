# 🔍 DocID Reference Tool

**Status:** Microservice (UI Tool)  
**Framework:** Flask + Tabulator.js  
**Internal Port:** `8002`  
**External Port:** `8003`  

---

## 1. Overview

The **DocID Tool** is a high-performance reference table used by the data operations team to quickly look up `ShowId`, `TheatreId`, and `DocumentTypeId` mappings from the SQL Server `TicketingDS` database. 

It replaces slow, manual SQL queries with a lightning-fast, searchable web interface that utilizes **Server-Side Caching** and **Virtual DOM rendering**.

### Key Features

- **30-Minute Memory Cache:** Data is fetched from SQL once every 30 minutes and stored in memory to ensure sub-millisecond response times.
- **Ultra-Fast Search:** Uses Tabulator's virtual DOM to filter through thousands of records instantly without page reloads.
- **Manual Resync:** Includes a "Resync" button to force an immediate refresh of the SQL data cache.
- **Debounced Input:** Search queries are debounced (500ms) to prevent UI lag during intensive filtering.

---

## 2. Technical Stack

- **Backend:** Python 3.11, Flask
- **Database:** `pyodbc` (Microsoft ODBC Driver 18)
- **Frontend:** Tailwind CSS, Tabulator.js (Semantic UI Theme)
- **Deployment:** Docker (Debian Bookworm Slim)

---

## 3. Configuration (.env)

The service requires the following environment variables to be mapped via the central `.env` file:

```env
SQL_SERVER=your_server_address
SQL_USERNAME=your_username
SQL_PASSWORD=your_password
```

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
The caching logic is located in `app.py` under the `DOCID_CACHE` dictionary and the `update_docid_cache()` function.

### Local Rebuild
```bash
docker-compose up -d --build docid-tool
```

### Logs
```bash
docker logs -f docid-tool
```
