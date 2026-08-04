# Code Review — `powerbi_refresher`

**Reviewed:** 4 August 2026
**Scope:** `app.py` (271 lines), `templates/dispatcher.html`, `Dockerfile`, `requirements.txt`, `PowerBI_Report_IDs.json`, plus the service's block in `/opt/prefect/prod/code/docker-compose.yml`.

## Deployment context

This review is prioritised for the actual deployment, not a generic public-facing Flask app:

- **Locally hosted on a Linux server reachable only over the company VPN.** There is no anonymous internet exposure.
- **Fewer than 10 concurrent users** across all sites and apps on the box.
- Deployed via `docker compose` with `--workers 4 --threads 8` (`Dockerfile:17`).
- The repo is pushed to **GitHub** (`origin` → `nateadewynters-prog/prefect-prod.git`).
- The deploy workflow runs `git reset --hard` and `git clean -fd` (per the `.gitignore` comment), so runtime state must live in an ignored path.

**What that context changes.** Two things move down: hardening against untrusted traffic (the VPN is doing that job) and contention-driven scaling concerns (10 users will not stress SQLite or Gunicorn). Two things move up: **anything that loses data or reports a false success**, because with this few users there is no redundancy and no second pair of eyes — a wrong number goes straight to a client — and **the client GUIDs committed to a GitHub-hosted repo**, which is the one exposure the VPN does not cover.

It also makes finding 1 far cheaper to fix than it first appears: at <10 concurrent users, `--workers 1 --threads 16` is ample capacity, and that single line eliminates the entire class of multi-process state bugs.

## Summary

For ~270 lines this does a lot right: MSAL client-credentials is used correctly, discovery is 2 API calls per workspace rather than N, SQLite gives cross-process state instead of module globals, and SSE is a genuinely good fit for a long-running refresh.

The problems cluster in one place: **the app is written as if it were a single process, but deployed as 4 independent Gunicorn workers.** Findings 1 and 9 both fall out of that, and finding 1 can refresh the wrong client's dataset. Fix 1–5 first; 1, 4 and 6 are one-liners.

---

## Top 10 — by priority for this deployment

### 1. Report IDs are positional and per-worker — a click can refresh the wrong dataset

`app.py:89` assigns each report an ID of `str(len(reports))`, i.e. its position in the discovery list. That ID is what the browser sends back to `/stream/<report_index>` (`app.py:201`), which resolves it against `_reports_config` — a **process-local** global (`app.py:45`). With `--workers 4` there are four independent lists, four `init_db()`/`sync_reports()` runs at boot (`app.py:164-166`), and four midnight sync threads.

- `GET /` is served by worker A and renders A's indices. The subsequent `GET /stream/7` is load-balanced to worker B, where index 7 may be a different report. The user clicks *Disney — Weekly Sales* and the tool refreshes something else, silently and successfully.
- `POST /api/reload-reports` (`app.py:195`) syncs **only** the worker that received it. The other three stay stale, and the page reloads 4 s later (`dispatcher.html:272`) against an arbitrary worker.
- Positional IDs are unstable even single-worker: any sync between page load and click re-indexes everything (`app.py:109`).

**Fix, in two parts.** Immediately, set `--workers 1 --threads 16` in `Dockerfile:17` — at <10 concurrent users that is more than enough headroom, and it kills the cross-worker divergence outright. Note that SSE streams hold a thread for the duration of a refresh, so keep threads generous. Then do the real fix: make the identifier intrinsic rather than positional — use `dataset_id` (or `f"{workspace_id}:{dataset_id}"`) as the route parameter and drop the `id` field entirely. The stream handler then needs no shared list at all.

### 2. "Refresh completed" can be reported for a refresh that never ran

`app.py:239-254` triggers the refresh, then polls `GET .../refreshes?$top=1` and treats the top row's `status` as its own. It isn't necessarily:

- Power BI enqueues asynchronously. If the new entry hasn't appeared on the first poll, `$top=1` returns the **previous** refresh — typically `Completed`. The loop breaks, stamps `last_refresh`, and reports success. The dataset is stale but the UI says otherwise, and stale numbers reaching a client is the expensive failure mode here.
- The mirror case: a previously `Failed` entry produces an immediate false failure.
- A scheduled refresh or another user's trigger firing concurrently produces the same confusion.

**Fix:** the 202 response carries a `RequestId` header — capture it and only accept poll rows whose `requestId` matches:

```python
resp = requests.post(refresh_url, headers=headers, json={}, timeout=30)
resp.raise_for_status()
request_id = resp.headers.get("RequestId")
...
rows = poll.json().get("value", [])
row = next((r for r in rows if r.get("requestId") == request_id), None)
if row is None:            # not enqueued yet — keep waiting, don't read row 0
    yield msg("⏳ Queued…"); time.sleep(5); continue
```

Poll `?$top=5` so a concurrent refresh can't push yours out of view. If `RequestId` is absent, fall back to requiring `startTime` later than a timestamp taken just before the POST. Also surface `serviceExceptionJson` from the failed row — `"❌ Refresh failed in Power BI."` (`app.py:256`) gives the user nothing to act on.

### 3. `PowerBI_Report_IDs.json` puts client workspace GUIDs on GitHub — the one thing the VPN doesn't cover

This 65 KB file is **dead code**: nothing in the repo references it (grep-confirmed), and `discover_reports()` superseded it. The comment at `app.py:42` still refers to the long-removed `get_report_ids.py`.

It is also a client-by-client map of workspace names, report names and GUIDs — Crossroads Live and others — and it is tracked in git and pushed to `github.com/nateadewynters-prog/prefect-prod`. Everything else in this review sits behind the VPN; this file does not. It reached a third-party host the moment it was committed, in `7da8a6f`, and it is present on `main` and every active branch. I haven't verified the repo's visibility setting, and that's rather the point — the exposure doesn't depend on it being public to be worth removing.

**Fix:** `git rm` the file and commit — that stops further distribution. Purging it from history is a separate decision (it needs a force-push and coordination across the seven branches that contain it); whether that's warranted depends on how sensitive you consider a workspace/report GUID map, which is a call for you rather than me. Worth confirming the repo is private either way, and adding a `.dockerignore` so build context stops carrying it.

### 4. The SQLite database is never persisted — every rebuild wipes refresh history

`sqlite3.connect("refresher_state.db")` (`app.py:134`) resolves to `/app` inside the container, and the compose block (`docker-compose.yml:115-125`) mounts only `.env`. There is no volume. Every `docker compose up --build` silently discards all logs and all `last_refresh` history — the "Last refreshed" line on every card resets to `—`, which is precisely the information a user checks before deciding whether to refresh.

The sibling service already solved this: `powerbi_media_report_dispatcher/config.py:40` reads `DB_PATH` from the environment, and its compose block mounts `./data`. Copy it exactly:

```python
DB_PATH = os.getenv("DB_PATH", "refresher_state.db")
```
```yaml
volumes:
  - /opt/prefect/prod/.env:/app/.env:ro
  - ./data:/app/data
environment:
  - DB_PATH=/app/data/refresher_state.db
```

Use `data/` specifically, not another path: `.gitignore` covers `data/*.db`, and the deploy workflow's `git clean -fd` would delete a DB written anywhere else in the tree.

### 5. Locks can stick permanently, and only a container restart clears them

- **Missing timeout.** `requests.post(refresh_url, ...)` at `app.py:239` is the only HTTP call in the file without `timeout=`. It can hang a worker thread indefinitely while holding the lock.
- **Unbounded poll loop.** `while True` (`app.py:242`) has no iteration or wall-clock cap. Gunicorn's `--timeout 900` kills the worker on any dataset taking >15 min; the worker is killed, not unwound, so the `finally` at `app.py:266` never runs and the lock stays set. `init_db()` clears locks (`app.py:143`) — but only at startup. That dataset then cannot be refreshed by anyone until someone thinks to restart the container.
- **Check-then-set race.** `is_locked()` (`app.py:214`) and `set_lock()` (`app.py:217`) are separate transactions, so two clicks a few ms apart both pass the check and both trigger.

The small user base makes this *worse*, not better: there's no one else to route around a wedged dataset, and the recovery step is a `docker compose restart` that a non-technical user won't attempt.

**Fix:** add `timeout=30` to the POST; cap the loop (e.g. 120 iterations / 10 min, then report "still running in Power BI" and release); store `locked_at` and treat locks past a TTL as stale; and make acquisition atomic so the caller knows whether it won:

```python
def try_acquire(dataset_id, ttl_seconds=1800):
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO locks (dataset_id, is_locked, locked_at) VALUES (?, 1, datetime('now')) "
            "ON CONFLICT(dataset_id) DO UPDATE SET is_locked = 1, locked_at = datetime('now') "
            "WHERE locks.is_locked = 0 OR locks.locked_at < datetime('now', ?)",
            (dataset_id, f"-{ttl_seconds} seconds"),
        )
        return cur.rowcount == 1
```

### 6. No fail-fast on configuration

`AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` (`app.py:9-11`) are read with no validation. If `.env` fails to mount — a live risk, since it's a bind mount of a single file from outside the build context (`docker-compose.yml:121`) — `TENANT_ID` becomes `None`, the authority becomes `https://login.microsoftonline.com/None`, and the first symptom is `Auth failed: None` mid-stream, after the UI has already rendered an empty page and the user has already clicked. Five lines turns a confusing runtime failure into an obvious boot failure:

```python
missing = [k for k in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET") if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")
```

### 7. Discovery drops reports silently — no pagination, no retry, and it blocks startup

`/groups`, `/reports` and `/datasets` responses are treated as complete (`app.py:58`, `app.py:80`, `app.py:84`) with no `$skip`/`$top` paging. Past the page limit, workspaces simply vanish from the UI with no error shown — the failure looks like "that report isn't in the tool", not like a bug.

There's also no 429/`Retry-After` handling and no retry on transient 5xx. A single blip drops reports until the next sync, and `sync_reports` swallows the exception into a log line (`app.py:115-117`). Separately, `sync_reports(log=False)` runs at import (`app.py:165`), so each worker boot serially walks every workspace at up to 30 s per call before serving anything — with enough workspaces that trips Gunicorn's boot timeout. (Reducing to one worker per finding 1 removes the 4× duplication of this work.)

**Fix:** page with `$skip`/`$top` until a short page returns; use a `requests.Session` with a `Retry` adapter honouring `Retry-After`; serve from the cached list immediately and sync in the background; and on failure keep the previous cache *and* flag staleness in the header rather than only writing to the log.

### 8. Timestamps are an hour off, and the logs table grows without bound

`datetime('now', 'localtime')` (`app.py:141`, `app.py:250`) and `datetime.now()` (`app.py:112`, `app.py:182`) resolve to the **container's** local time, which is UTC — no `TZ` is set for this service in compose. For UK users in British Summer Time, "Last refreshed 14:32" (`dispatcher.html:179`) currently reads an hour early. On a tool whose entire purpose is answering "is this data fresh?", a silently wrong clock undermines the one number users rely on. Set `TZ=Europe/London` in the compose block, or store UTC throughout and let the browser convert (`Date` handles it correctly given ISO-8601 with a `Z`).

`db_log` (`app.py:157`) also inserts a row per SSE message with no retention, while `/api/state` only ever reads the last 30 minutes (`app.py:182`) — so a dozen-plus rows per refresh accumulate permanently on a box you're already persisting to disk. Prune on write or during the daily sync, and add `CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(timestamp)`.

### 9. SQLite handling: connections are never closed, and WAL isn't enabled

`with get_db_conn() as conn:` (`app.py:139`, `146`, `153`, `158`, `180`, `187`, `248`) does **not** close the connection — sqlite3's context manager commits or rolls back the *transaction* and leaves the handle open until GC. `/api/state` opens two connections where one would do (`app.py:180`, `app.py:187`), and every open tab hits it every 2 s (`dispatcher.html:226`).

At <10 users this is churn rather than crisis, which is why it sits at 9 rather than 3 — but it's cheap to get right, and WAL removes any residual `database is locked` risk from concurrent pollers and the background sync thread. Wrap access in one `@contextmanager` helper that commits *and* closes, and set the pragmas once in `init_db()`:

```python
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA busy_timeout=5000")
```

### 10. One file, no tests — which is what makes findings 1–5 hard to fix safely

`app.py` mixes config, MSAL auth, Power BI API calls, persistence, background scheduling and HTTP routing, so nothing is testable without a live tenant. There are no tests, type hints, or lint config. That's the real cost here: the fixes above are subtle (request-ID matching, atomic locking) and there's currently no way to verify them except clicking buttons in production against real client datasets.

`powerbi_media_report_dispatcher` already has the split to copy — `config.py`, `state.py`, `services/powerbi.py`:

```
config.py            # env parsing + fail-fast validation (finding 6)
state.py             # DB helper, locks, logs, last_refresh
services/powerbi.py  # token, paged discovery, trigger + poll-until-done
app.py               # routes only
```

The two units worth testing first are exactly the two worst bugs: request-ID matching against a fake `/refreshes` payload (finding 2), and atomic lock acquisition under concurrent callers (finding 5).

---

## Honourable mentions

**Downgraded by the deployment context** — noted so the reasoning is on record rather than because I'd action them now:

- **No application-level authentication.** In a public deployment this would be top-three: port 8004 is published to the host (`docker-compose.yml:118-119`) with no proxy or auth, exposing every workspace and report name the service principal can see and allowing anyone to trigger refreshes. On a VPN-only box with <10 trusted users, the network boundary is doing this job and adding auth is not a good use of effort. Two residual pieces are still worth the small effort: **`/stream/<id>` is a state-mutating `GET`**, so a prefetcher, link scanner or browser history restore can fire a real refresh — make it a `POST` that returns a job ID, with SSE reading from a separate stream. And **there's no attribution**: the logs table records what happened but not who did it, so with no auth you can't answer "who kicked off that refresh at 08:40?" A proxy-supplied username written into `logs` would close that.
- **DOM-based XSS via report names.** `dispatcher.html:195` builds the status bar with `innerHTML` from text read out of the DOM at `dispatcher.html:191`. Jinja escapes the name on render, but `textContent` un-escapes it and re-inserting as `innerHTML` executes it — so a report named `<img src=x onerror=…>` runs script. Real, but it requires someone to name a report that way inside your own tenant and the blast radius is ten colleagues on a VPN. Still a one-line fix: build the tag with `document.createElement` and set `textContent`.
- **Dependency pins are ~2 years old** (`requirements.txt`) with no scanning; `requests==2.31.0` and `gunicorn==21.2.0` both have published advisories against those exact versions, and `urllib3` is unpinned and transitive. The Gunicorn one concerns request smuggling, which needs a hostile client — largely mitigated here. Bump on the next touch rather than urgently.
- **No `HEALTHCHECK`** (`Dockerfile`), so `restart: unless-stopped` can't detect a wedged app. With a handful of users someone will just report it, but a `/healthz` checking DB reachability and cache age is cheap and pairs well with finding 5.
- **Container runs as root.** Add a non-root `USER`. Defence in depth on a trusted network.

**Still worth doing, independent of exposure:**

- **Per-dataset locks, but the UI blocks globally.** `dispatcher.html:211` puts every other button into `Waiting…` whenever any lock exists, discarding the concurrency the per-dataset design provides. With under 10 users, two people wanting different datasets at once is a routine collision, not a rare one — so this one is arguably *more* annoying at this scale. Only disable the locked card, or make the copy honest about serialising.
- **Failed refreshes send no `[DONE]` sentinel** (`app.py:257`, `app.py:262`), so the client relies on connection close and `es.onerror` — a genuine network drop and a Power BI failure look identical in the UI. Emit `[DONE]` on every path.
- **A failed midnight sync waits a full day to retry** (`app.py:124`). Add a short backoff-and-retry on failure.
- **No way to cancel**, and no visibility into a refresh someone else started before this page was opened.
- **Reports are never sorted**, so workspace groups appear in arbitrary API order (`app.py:88`, grouped at `dispatcher.html:93`). Sorting by `(workspace_name, report_name)` makes the list stable between syncs and quicker to scan.
- **`_reports_synced_at` is read outside `_reports_lock`** (`app.py:175`, `app.py:192`), unlike `_reports_config`. Benign under CPython, but the lock exists — use it.
- **Dataset IDs truncated to 8 chars** (`dispatcher.html:102`, `dispatcher.html:193`) are the only disambiguator when two reports share a name. A `title` attribute with the full GUID costs nothing.
- **Tabler Icons is loaded from a CDN at `@latest`** (`dispatcher.html:6`) — unpinned, no SRI, and it breaks the UI if the box has no outbound internet access, which is worth checking on a VPN-only host. Pin the version or vendor it into the image.
- **Fixed 5 s poll interval** (`app.py:258`) regardless of dataset size. Mild backoff (5 s → 15 s → 30 s) cuts API calls on long refreshes without hurting short ones.
- **The token is fetched once per stream** (`app.py:230`) and reused for the whole poll loop; a refresh approaching token lifetime will start 401-ing mid-poll. Call `pbi_headers()` inside the loop — MSAL's cache makes it nearly free.
