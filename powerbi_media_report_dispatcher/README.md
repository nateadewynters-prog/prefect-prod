# Power BI Media Report Dispatcher

A small Flask tool that dispatches the **weekly digital media reports**
(spend / revenue / ROAS from BigQuery + a Power BI PPTX and PNG preview,
emailed via MS Graph). It's a companion to `powerbi_sales_report_dispatcher`
and is built to look and behave the same way.

## Folder layout

```
powerbi_media_report_dispatcher/
├── app.py                 # backend: config, BigQuery, Power BI export, email, SSE
├── templates/
│   └── dispatcher.html    # the board UI
├── Dockerfile
├── requirements.txt
└── README.md
```

## The one file you edit regularly

`SHOWS_CONFIG` in `app.py` — one entry per show. To add a show, copy an entry
and change `code`, `show_name`, `gbq_name`, the Power BI IDs, and `recipients`.

**The Devil Wears Prada is left unconfigured on purpose** — its uploaded script
still had `xxxx` placeholder IDs. Its card shows "Not configured yet" and the
Dispatch button is disabled until you fill in `workspace_id`, `report_id` and
`dashboard_url`.

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

3. **ROAS is shown as `3.42x`, not `£3.42`.** The per-show scripts printed
   `£{roas}`, which was a copy/paste leftover from the spend/revenue lines —
   ROAS is a ratio. If recipients are used to seeing the `£`, revert the two
   ROAS lines in `build_email_html` / `/metrics`.

4. **Global lock, one at a time.** Power BI limits concurrent exports, so the
   tool refuses to start a second dispatch while one is running (same as sales).

5. **Its own database file.** The media tool uses `media_dispatcher_state.db`,
   NOT the sales tool's `dispatcher_state.db`. They both mount `./data`, but
   sharing one DB breaks things: the schemas differ (you get "Error loading
   history"), and the shared `locks`/`logs` tables would make the two tools
   block each other. Keep the two `DB_PATH` values distinct.

6. **DWP will stay disabled** until you add its Power BI IDs.

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