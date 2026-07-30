# Power BI Media Report Dispatcher

A small Flask tool that dispatches the **weekly digital media reports**
(spend / revenue / ROAS from BigQuery + a Power BI PPTX and PNG preview,
emailed via MS Graph). It's a companion to `powerbi_sales_report_dispatcher`
and is built to look and behave the same way.

## Folder layout

```
powerbi_media_report_dispatcher/
├── app.py                 # thin entrypoint: builds the app, exposes `app` for gunicorn
├── config.py              # env vars, constants (STAGES), SHOWS_CONFIG  ← the file you edit
├── state.py               # SQLite: locks, logs, dispatch history
├── pipeline.py            # the dispatch/refresh jobs + shared SSE plumbing
├── routes.py              # all HTTP endpoints (Flask Blueprint)
├── services/
│   ├── auth.py            # Azure AD tokens (MSAL)
│   ├── bigquery.py        # spend / revenue / ROAS
│   ├── powerbi.py         # report export + dataset refresh
│   └── email.py           # email HTML + MS Graph send
├── templates/
│   └── dispatcher.html    # the board UI
├── Dockerfile
├── requirements.txt
└── README.md
```

Previously everything lived in one ~560-line `app.py`; it's now split into
small, single-purpose modules so each file can be read and debugged on its own.
The behaviour is unchanged — same routes, same JSON, same SSE format.

### Where does X live?

| I want to…                                   | Look in                     |
|----------------------------------------------|-----------------------------|
| add / edit a show, recipients, Power BI IDs  | `config.py` (`SHOWS_CONFIG`)|
| change an env var / secret name              | `config.py`                 |
| touch the locks / logs / history tables      | `state.py`                  |
| change the email wording or layout           | `services/email.py`         |
| change the BigQuery query                    | `services/bigquery.py`      |
| change the Power BI export / refresh calls   | `services/powerbi.py`       |
| change Azure auth / scopes                   | `services/auth.py`          |
| change the dispatch/refresh steps or logs    | `pipeline.py`               |
| add / change a URL endpoint or its JSON      | `routes.py`                 |
| change how the app boots (gunicorn `app:app`)| `app.py`                    |
| change the board UI                          | `templates/dispatcher.html` |

## The one file you edit regularly

`SHOWS_CONFIG` in `config.py` — one entry per show. To add a show, copy an entry
and change `code`, `show_name`, `gbq_name`, the Power BI IDs, and `recipients`.

**The Devil Wears Prada is now configured** — it has real `workspace_id`,
`report_id` and `dashboard_url` values; only its `recipients` list is still
narrowed to `a.cameron@dewynters.com`. Any show left with empty Power BI IDs
shows "Not configured yet" on its card and its Dispatch button stays disabled
until you fill in `workspace_id` and `report_id`.

## Ports

Your `docker-compose.yml` already uses 4200, 80, 8002, 8003, 8004, 9000, 9443.
The next free one is **8005**, so the container's internal 8002 is mapped to
host 8005 below (same trick you used for `docid-tool`).

## docker-compose service block

Add this under `services:` in the main `docker-compose.yml`:

```yaml
  # --- 10. POWER BI MEDIA REPORT DISPATCHER (UI TOOL) ---
  powerbi-media-report-dispatcher:
    build: ./powerbi_media_report_dispatcher
    container_name: powerbi-media-report-dispatcher
    ports:
      - "8005:8002"
    volumes:
      - /opt/prefect/prod/.env:/app/.env:ro
      - ./data:/app/data
      # BigQuery service-account key, mounted read-only. This file already
      # lives here on dew-insights01. It MUST be a real file, not a directory —
      # create it before the first `up`, or Docker makes an empty dir stub.
      - /opt/prefect/prod/keys/dewynters-6a2afc43a47e.json:/app/service_account.json:ro
    environment:
      # IMPORTANT: use a different DB file from the sales dispatcher.
      # They can't share dispatcher_state.db — the schemas differ, and shared
      # locks/logs would make the two tools block each other.
      - DB_PATH=/app/data/media_dispatcher_state.db
      - GOOGLE_APPLICATION_CREDENTIALS=/app/service_account.json
      - GBQ_PROJECT_ID=dewynters
    restart: unless-stopped
    logging:
      driver: "json-file"
      options: { max-size: "10m", max-file: "3" }
```

Then: `docker compose up -d --build powerbi-media-report-dispatcher`

## Gotchas worth knowing (things I changed from the original scripts)

1. **BigQuery credentials are no longer a laptop path.** The scripts hard-coded
   `/home/alexc/.../dewynters-....json`, which doesn't exist inside a container.
   The app now reads `GOOGLE_APPLICATION_CREDENTIALS` from the environment, and
   compose mounts the key file in. Put the real key on the host at the path in
   the volume line above (or change that path to wherever the key actually lives).

2. **Azure secrets come from the shared `.env`** (`/opt/prefect/prod/.env`),
   mounted read-only, exactly like the sales dispatcher.

3. **ROAS is shown as `£3.42`, not `3.42x`.** `build_email_html` and `/metrics`
   both print `£{roas}`, kept from the per-show scripts to match what recipients
   are used to seeing — even though ROAS is a ratio, not an amount. The live SSE
   log in `pipeline.py` is the one place it reads `3.42x`. If you want the `x`
   form everywhere, change the two ROAS lines in `build_email_html` / `/metrics`
   (and the History modal in `dispatcher.html`).

4. **Global lock, one at a time.** Power BI limits concurrent exports, so the
   tool refuses to start a second dispatch while one is running (same as sales).

5. **Its own database file.** The media tool uses `media_dispatcher_state.db`,
   NOT the sales tool's `dispatcher_state.db`. They both mount `./data`, but
   sharing one DB breaks things: the schemas differ (you get "Error loading
   history"), and the shared `locks`/`logs` tables would make the two tools
   block each other. Keep the two `DB_PATH` values distinct.

6. **DWP emails one person.** Its Power BI IDs are filled in, but unlike the
   other shows its `recipients` list doesn't include `BASE_RECIPIENTS` — widen it
   in `config.py` when it should go to the full list.

## Running locally (without Docker)

```bash
pip install -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
# plus AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET in a .env
python app.py          # serves on http://localhost:8002
```

## How to debug

- **Watch a run live:** open the card's "Detailed log" while it dispatches, or
  `docker logs -f powerbi-media-report-dispatcher`.
- **Test data only, no email:** click **Media Figures** (runs the BigQuery read)
  or **Email Preview** (builds the HTML) — neither sends anything.
- **Reset state:** delete `./data/media_dispatcher_state.db` and restart. Locks
  also auto-clear on boot, so a crashed run won't wedge the tool.