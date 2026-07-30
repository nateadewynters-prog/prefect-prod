# DocID Tool — Code Analysis

**Date:** 30 July 2026
**Branch:** `analysis/docid-tool-review`
**Scope:** `docid_tool/` only — `app.py`, `templates/docid.html`, `Dockerfile`, `requirements.txt`, `README.md`
**Status:** Report only. No code changes made.

Findings from five parallel reviews (correctness, data integrity, security, code quality, test coverage), de-duplicated and ranked. Every claim below was verified against the code; items that turned out not to be defects are listed in section 6 so they are not re-investigated.

**The service is currently down in production.** `GET /api/docids` has returned HTTP 500 since commit `abaa21e` (30 July, 09:46). Confirmed live: `curl -i http://127.0.0.1:8003/api/docids` → `500 INTERNAL SERVER ERROR`.

**Totals:** 2 critical · 8 high · 15 medium · 17 low = **42 findings**

Fixes marked **[D]** need a decision from you before implementation; **[S]** are safe, mechanical changes. Fixes marked **[X]** require edits outside `docid_tool/` (`docker-compose.yml`, `.gitignore`, `.github/workflows/deploy.yml`) and are therefore out of the agreed scope — flagged for your call.

---

## 1. Critical

### C1 — Half-finished cache migration; every API request fails
`app.py:105-107, 127-138` · **[S]**

Commit `abaa21e` ("Comments Updated", +62/−7) removed `import threading`, the `DOCID_CACHE` dict and `CACHE_LOCK`, added a SQLite cache layer (lines 25–69), and never rewired the two functions that used the old globals. `read_cache()` and `write_cache()` are defined but called by nothing. `CACHE_LOCK` and `DOCID_CACHE` are referenced at eight sites and bound nowhere.

The two routes fail differently, which matters for triage:

| Route | Behaviour now |
|---|---|
| `GET /` | **Works.** Page renders. A casual "is it up?" check passes — likely why this shipped. |
| `GET /api/docids` | `NameError` at line 127, **outside** any `try` → unhandled → Flask 500 HTML. Every request. |
| `POST /api/docids/refresh` | Connects to SQL, **runs the full query successfully**, then hits `NameError` at line 105 **inside** the `try`. Swallowed by `except Exception` and returned as `{"status":"error","message":"name 'CACHE_LOCK' is not defined"}`. |

Two consequences beyond the outage: the log line reads `SQL Error fetching DocIDs: name 'CACHE_LOCK' is not defined`, sending whoever debugs it after SQL credentials, the ODBC driver and firewall rules — all of which are healthy. And every Resync click still costs a full table read against production `TicketingDS` before failing.

**Fix:** complete the migration (~15 lines). Replace lines 105–107 with `write_cache(fresh_data)`; replace the cache check in `api_docids()` with `rows, updated_at = read_cache()` and a TTL test against `CACHE_TTL_SECONDS`. Do not leave both halves in the tree.

### C2 — ID coercion collapses NULL, 0 and empty string into one value
`app.py:86-88` · **[D]**

`int(r[3]) if r[3] else 0` tests truthiness, not nullness:

| Source value | Result | |
|---|---|---|
| `NULL` | `0` | |
| `0` (genuine ID zero) | `0` | falsy → takes the `else` branch |
| `''` | `0` | |
| `Decimal('0')` | `0` | |
| `'  '` (whitespace) | **`ValueError`** | see H4 |
| `'ABC'` | **`ValueError`** | see H4 |
| `Decimal('12.7')` | `12` | silent truncation |

Three distinct source states become one output value, and that value is itself a legal ID. Two silent failures follow:

1. **Records merge.** A row with `ShowId = NULL` and a row with `ShowId = 0` produce the same dedupe key. One is dropped; which one depends on an unstable sort (H1). A distinct real record is destroyed by a type coercion.
2. **A row is fabricated.** All-NULL IDs yield the key `(0,0,0)` and are appended as a real-looking record — typically `Hamilton | N/A | N/A | 0 | 0 | 0`. It does not exist in the source, and because every all-NULL row collapses into it, the fabrication is simultaneously a mass deletion. A user copies `ShowId 0` downstream in good faith.

A `0` member meaning "Unknown"/"Not Applicable" is a standard warehouse dimension convention, so a genuine `0` in `TicketingDS` cannot be ruled out from the code — and nothing in the repo asserts otherwise.

**Fix:** stop coercing. Return `None` for missing IDs and let it serialise to JSON `null`; render as an em dash in the UI, never as a number. `None` is distinct from `0` in a Python tuple, so NULL rows can no longer merge with real zeros.
**Decision needed:** this changes the API payload and the UI. Confirm whether a real `ShowId`/`TheatreId`/`DocumentTypeId` of `0` exists — a one-line `SELECT COUNT(*) ... WHERE ShowId = 0 OR TheatreId = 0 OR DocumentTypeId = 0` settles it and should become a standing check.

---

## 2. High

### H1 — Dedupe key excludes the display names; the surviving row is non-deterministic
`app.py:78, 91-103` · **[D]**

The key is `(show_id, theatre_id, doc_type_id)`, but the row kept carries `ShowName`, `TheatreName` and `DocumentName`. Where two source rows share an ID triple and differ in any name, the first encountered wins and the rest are discarded — no count, no log, no signal in the response. `[dbo].[DocumentsAndVenues]` is a documents-and-venues join, so a repeated ID triple under different `DocumentName`s is the expected shape, not an anomaly.

Worse, `ORDER BY ShowName` is a sort on one non-unique column. SQL Server guarantees no ordering among ties, so the winner can change between refreshes with no change to the data. The visible Document Name against a given ID triple can silently flip from one 30-minute refresh to the next.

This is the worst failure mode for a lookup tool: users validate their ID choice by reading the *name* beside it, and that name may belong to a different sibling row. Someone screenshots a row on Monday and sees something different on Wednesday.

**Fix:** dedupe on the full displayed tuple, or aggregate rather than drop — collapse names into a distinct list plus a `variants` count so the collision is visible. Either way make the ordering total (`ORDER BY ShowName, TheatreName, DocumentName, ShowId, TheatreId, DocumentTypeId`) and log `len(rows) - len(fresh_data)` on every refresh so silent shrinkage is detectable.
**Decision needed:** should the tool show every source row, or one row per ID triple? They are different products.

### H2 — No test coverage and no CI gate; nothing can block a broken deploy
`docid_tool/` (no tests) · `.github/workflows/deploy.yml` · **[X]**

No tests exist in `docid_tool`, and no repo-wide pytest config exists anywhere. One sibling — `sales_report_extraction` — does have a competent 11-file pytest suite using `unittest.mock`, so the house convention exists; it has simply never reached this service. Four of six services have no tests.

`deploy.yml` has three steps: Checkout → `git reset --hard origin/main` → `docker compose up -d --build`. There is no test step, and deploy is unconditional. Note the ordering problem: step 2 rewrites the production filesystem *before* any gate could run, so a test step added after it would run too late to protect production.

Note that `python -c "import app"` would **not** have caught C1 — the undefined names are inside function bodies. `ruff`/`pyflakes` would have, in under a second.

**Fix:** six tests would have stopped this. Smoke-test all three routes with `pyodbc` mocked; assert `/api/docids` returns a JSON array of 6-element lists; cold-cache populate-and-serve; ID coercion (parametrised, including "one bad row must not destroy the dataset"); TTL plus stale fallback; SQLite round-trip and upsert. Put `pytest` in a separate `requirements-dev.txt` — not `requirements.txt`, which the Dockerfile installs into the production image. A `conftest.py` must set `DOCID_CACHE_DB` to a tmp path and stub `sys.modules["pyodbc"]` *before* importing `app`, because `init_db()` runs at import.
**Out of scope:** the CI change and its correct placement (between Checkout and Sync) are in `.github/`.

### H3 — Failed refresh triggers an unbounded retry storm
`app.py:124-138` · **[D]**

`last_updated` is written only on success. After a failure, `needs_update` stays `True` for every subsequent request, so every `GET /api/docids` re-attempts the SQL connection. Each attempt blocks for the full 10s login timeout. With four **sync** gunicorn workers, four concurrent page loads consume the entire pool for 10s+ each — the service stops answering even `GET /`. A brief DB blip becomes a total outage, and it does not self-limit.

**Fix:** record `last_attempt` alongside `updated_at` and skip re-fetching within ~60s of a failure, serving stale data (or 503 with `Retry-After`) meanwhile.

### H4 — One malformed row aborts the entire refresh
`app.py:86-88, 110-112` · **[S]**

`ValueError` from `int('  ')` or `int('ABC')` escapes the per-row loop, is caught by the function-level handler, and the whole refresh returns failure — discarding every successfully-read row. One dirty cell in a table this app does not own takes the reference table offline for the whole team, indefinitely, under the message `SQL Error fetching DocIDs: invalid literal for int()`. Combined with H3, the app then re-queries SQL on every request forever.

**Fix:** coerce per row, collect failures, keep going. Log the offending row and surface a `skipped_rows` count.

### H5 — Backend failures render as an empty table, not an error
`templates/docid.html:83-96` · **[S]**

`ajaxResponse` calls `response.map(...)` unconditionally, and the Tabulator config declares no `dataLoadError`/`ajaxError` handler and no `placeholder`. Verified against the Tabulator 6.2.1 source: a failed load logs to console, shows an alert for 3 seconds, then clears — leaving an empty grid with no explanation, permanently. The only error feedback anywhere is `alert("Failed to refresh database.")`, which fires solely for the manual Resync POST, never for the initial load.

This is why C1 has been live since 30 July without an accurate bug report: users see "no data" and conclude the mapping does not exist. A silent false negative is more dangerous than a visible error, because users act on it.

**Fix:** guard the handler (`if (!Array.isArray(response))`), add a `dataLoadError` callback rendering a persistent banner, and set `placeholder`.

### H6 — A successful-but-empty fetch will overwrite a good cache
`app.py:59-67`, call site to be added at 105 · **[D]**

The write itself is sound — a single atomic upsert, correctly pinned to one row by `CHECK (id = 1)`. But nothing guards the *content*. If the query succeeds and returns zero rows (a permissions change, a mid-reload source table, an upstream `WHERE`), `write_cache([])` replaces a complete cache with an empty one and reports `"Cache updated successfully."`

This is the one path that turns a transient upstream problem into persistent local data loss. The stale-data fallback (H7) only protects against *exceptions* — a successful-but-empty fetch bypasses it and destroys the fallback it depends on.

**Fix:** treat zero rows as a failure and leave the existing cache untouched. Consider also rejecting a refresh that drops more than ~50% of the previously cached row count.
**Decision needed:** the percentage threshold, and whether an override is wanted.

### H7 — Stale data is served with no staleness signal
`app.py:133-138`, `templates/docid.html:40-46` · **[D]**

Three defects in five lines:

1. **Wrong emptiness test.** `if not success and not DOCID_CACHE["data"]` uses an empty list as the proxy for "never populated", but the real signal is `last_updated is None` — which is what line 128 uses. The two branches disagree about what "populated" means.
2. **Inconsistent locking.** Lines 135 and 138 read the cache outside the lock that lines 127–131 take. Benign under sync workers; it bites the first time anyone adds `--threads`.
3. **Silence.** On failure with a warm cache, the response is an ordinary `200` with an ordinary array. `read_cache()` returns `updated_at` and the API discards it. The client cannot distinguish 5-second-old data from 3-day-old data, and the UI shows no age at all.

Combined with H4 (any single malformed ID kills every refresh) and the misattributed "SQL Error" log, the tool can serve data that is days old while looking perfectly healthy. For a table whose IDs are copied into other systems, undetectable staleness is the same class of harm as wrong values.

**Fix:** branch on `last_updated is None`; return an envelope `{"rows": [...], "updated_at": ..., "stale": bool}`; render "Last synced HH:MM" beside the Resync button and an amber banner past the TTL.
**Decision needed:** the envelope is a breaking change to the frontend contract — it requires the matching edit at `docid.html:85` in the same commit.

### H8 — `docid_cache.db` is not covered by `.gitignore`; deploy can overwrite it
`app.py:27-30`, `.gitignore:16` · **[X]**

`DB_PATH` defaults to `docid_tool/docid_cache.db`, inside the git working tree. A gitignore pattern containing a slash is anchored to the directory holding the `.gitignore`, so `data/*.db` matches only `<root>/data/*.db`. Verified: `git check-ignore docid_tool/docid_cache.db` exits 1 — **not ignored**.

This is not hypothetical. `sales_report_extraction/data/error_tracking/dataops_tracking.db` is **already tracked in git** for exactly this reason — a live SQLite database committed to the repo. Once `docid_cache.db` is committed by one `git add -A`, the deploy workflow's `git reset --hard origin/main` overwrites the live database with a stale snapshot on every deploy — verbatim the failure the `.gitignore` comment was written to prevent.

While it stays untracked, `git clean -fd` deletes it on every deploy instead.

**Fix:** point `DB_PATH` at the already-ignored root `data/` directory and bind-mount it, matching `powerbi_media_report_dispatcher` exactly (env var `DB_PATH`, `./data:/app/data` mount, explicit compose `environment:` entry). Separately, broaden the gitignore pattern to `*.db` and audit whether the tracked `dataops_tracking.db` should be `git rm --cached`ed.
**Out of scope:** requires `.gitignore` and `docker-compose.yml` edits.

---

## 3. Medium

| # | Location | Finding | Fix |
|---|---|---|---|
| M1 | `app.py:133-134, 143` | **No single-flight guard.** On TTL expiry every request that arrives before the first refresh completes runs its own full `SELECT` — up to 4 concurrent, 5 with a Resync click. Same on cold start. | Claim an exclusive refresh via `BEGIN IMMEDIATE` + a `refresh_started_at` column; losers serve stale data. **[D]** |
| M2 | `app.py:32-37, 41, 51, 61` | **Every SQLite connection is leaked.** `with sqlite3.connect(...)` manages the *transaction*; it never closes the connection. Once C1 is fixed, `read_cache()` runs per request and leaks an fd each time, until `unable to open database file`. | `with contextlib.closing(get_db_conn()) as conn, conn:`. Add `PRAGMA journal_mode=WAL` while there. **[S]** |
| M3 | `app.py:55-57` | **Corrupt cache is unrecoverable.** `json.loads` and `fromisoformat` are unguarded. A truncated write or format change raises out of `read_cache()` → 500. Self-perpetuating: every request calls `read_cache` before it can decide to refresh, so the repair path is never reached. Requires SSH to fix. | Catch, log, `DELETE FROM docid_cache`, return `([], None)` so the next request rebuilds. **[S]** |
| M4 | `app.py:110-112` | **`except Exception` spans connection, query, parsing and cache write**, relabelling every programming error as "SQL Error". This is what disguised C1 and misreports H4. | Narrow to `except pyodbc.Error`; handle parse failures per row; let genuine bugs raise. **[S]** |
| M5 | `app.py:112, 147` | **Raw exception text returned to the browser.** pyodbc errors routinely carry the SQLSTATE, driver version, DB username and sometimes the server hostname (`Login failed for user '<uid>'`). The password is not echoed, but this is unnecessary disclosure on an unauthenticated endpoint. The frontend discards the message anyway. | Return a fixed string; log the detail server-side. **[S]** |
| M6 | `app.py:111`, `Dockerfile` | **`print()` for errors, and no `PYTHONUNBUFFERED=1`.** No timestamp, level or traceback — and under gunicorn in Docker, stdout is a block-buffered pipe, so a short error line can sit unflushed. `docker logs -f docid-tool` — the exact command the README recommends — may show nothing while the tool is broken. Only `print()` in the repo. | `ENV PYTHONUNBUFFERED=1`; `logging.getLogger(__name__).exception(...)`. **[S]** |
| M7 | `app.py:75, 78` | **`timeout=10` bounds login only,** not query execution (`conn.timeout` is never set, so it is unlimited). A slow `SELECT` hangs the worker until gunicorn's `--timeout 120` SIGKILLs it mid-refresh — cache never written, client gets a dropped connection. | `conn.timeout = 30`, comfortably under the gunicorn timeout. **[S]** |
| M8 | `Dockerfile`, `app.py` | **No healthcheck and no health endpoint.** `restart: unless-stopped` only reacts to process exit. "Process alive, every API request 500ing" is precisely what Docker cannot see — which is why C1 ran unnoticed for a day. A healthcheck on `/` would *also* have missed it; it must hit the API. | Add `/health` returning cache age and last error; `HEALTHCHECK` against it (curl is already in the image). **[S]** |
| M9 | `docid_tool/` | **No `.dockerignore`.** `COPY . .` bakes in `__pycache__/` (present now, and compiled for 3.12 against a 3.11 image) and, once the app runs locally, `docid_cache.db` — shipping a developer's cache as production reference data. | Three lines: `__pycache__/`, `*.pyc`, `*.db`, `.env`. **[S]** |
| M10 | `templates/docid.html:7, 9, 71` | **Three CDN dependencies with no SRI.** `cdn.tailwindcss.com` is the dev-only JIT build and is entirely unversioned. If unpkg is unreachable, `new Tabulator(...)` throws and the page renders as a blank box with no error. A CDN or npm compromise injects arbitrary JS into a page staff trust. | Vendor Tabulator and a built Tailwind stylesheet into `static/`; failing that, add `integrity` + `crossorigin`. **[D]** |
| M11 | `app.py:116, 121, 140`; compose | **No authentication on any route,** including the state-changing POST. Confirmed exposed on `0.0.0.0:8003`. The POST is a "simple request" (no preflight), so any page a staff member visits can fire it in a loop — each call costing a full production SQL query across 4 workers. Data sensitivity is low; the uncontrolled load is the real issue. Note the whole estate follows this pattern, Portainer included, so this is platform-level. | Bind to `127.0.0.1` behind an authenticating proxy; rate-limit refresh via `updated_at`. **[D][X]** |
| M12 | `app.py:6`; compose | **The whole-estate `.env` is loaded into this process.** `/opt/prefect/prod/.env` is mounted into every service, so this container's environment holds every credential in the platform, not just the three it needs. | Pass only the three `SQL_*` vars via compose `environment:`; drop the mount and `load_dotenv()`. **[D][X]** |
| M13 | `app.py:96-103`, `docid.html:85-92` | **Positional list-of-lists couples the SQL column order to frontend indices.** Inserting a column into the `SELECT` silently mislabels every column in the UI — no exception, just shows displayed under theatre names. | Emit dicts; the frontend `ajaxResponse` mapping then collapses to `return response`. **[D]** |
| M14 | `app.py:97-99`, `docid.html:158` | **Rows with NULL names are unreachable via the dropdowns.** The backend maps NULL to the string `"N/A"`; `fillSelect` explicitly skips `"N/A"`. So the row exists but no dropdown can isolate it — a user filtering by show concludes the mapping does not exist. Compounds H1: if the dedupe winner is the NULL-named row, the whole ID triple becomes filter-invisible. | Send `null`, render a distinct placeholder, add an explicit "(no name in source)" option. **[D]** |
| M15 | `templates/docid.html:83-96, 144-164` | **`populateDropdowns` runs as a side effect of `ajaxResponse`,** rebuilding the selects on every load and wiping the user's selection via `innerHTML` — while the *applied* filter persists in the `setFilter` closure. Not reachable today only because `forceRefresh` happens to reset and `clearFilter()` first. One auto-refresh or retry away from showing a filtered subset while all dropdowns read "All". | Move to the `dataLoaded` callback; preserve and restore the selected value. **[S]** |

---

## 4. Low

| # | Location | Finding |
|---|---|---|
| L1 | `app.py:25` vs `130` | `CACHE_TTL_SECONDS = 1800` is never read; line 130 hardcodes `1800`. Anyone tuning the constant will see no effect. **[S]** |
| L2 | `app.py:99` | `# DocumentName[cite: 1]` — a citation artefact from an AI/document paste, the only occurrence in the repo. Three lines from C2 and H1. **[S]** |
| L3 | `README.md:14, 18, 20, 27, 64` | Documents the deleted design: "stored in memory", "the cache is per-worker", and directs maintainers to "the `DOCID_CACHE` dictionary". `DOCID_CACHE_DB` is undocumented. A maintainer reads "the cache is per-worker" and stops investigating a real staleness bug. **[S]** |
| L4 | `app.py:9-24` | The comment block asserts the SQLite cache is in use and explains why. Every claim describes code that never runs. It is the most confident text in the file, so it will be believed. Becomes true once C1 lands. **[S]** |
| L5 | `app.py:22-24` | "An empty cache behaves exactly like an expired one" is false where it matters: an expired cache still holds rows, so a SQL outage degrades to stale data; an empty one returns 500. Wiping the file on deploy guarantees the H7 fallback is unavailable exactly when most needed — and deploys correlate with infrastructure work. **[D]** |
| L6 | `templates/docid.html:155` | `id.replace('Filter','s')` yields "All shows", "All theatres" and **"All docTypes"**, overwriting the correctly-cased server-rendered labels on first data load. **[S]** |
| L7 | `templates/docid.html:174-193` | `forceRefresh` does not `await table.setData()`. The spinner stops when the POST resolves while the GET is still in flight; a second click makes Tabulator drop a response. (`setData()` with no arguments is correct — verified in the 6.2.1 source.) **[S]** |
| L8 | `templates/docid.html:175, 184-186` | The server's error message is fetched and discarded in favour of a generic alert. Right now it would read `name 'CACHE_LOCK' is not defined` — the one piece of evidence that identifies C1. **[S]** |
| L9 | `app.py:57, 66, 107, 124, 130` | Naive local-time datetimes stored with no offset. Latent today (container runs UTC); the moment anyone sets `TZ=Europe/London`, the BST→GMT shift makes the delta negative and the cache is treated as fresh for an extra hour. **[S]** |
| L10 | `app.py:69` | `init_db()` runs at import with no error handling, in all four workers. Works today only because the container runs as root on a writable layer — adding a `USER` or `read_only: true` crashes every worker at boot. **[S]** |
| L11 | `Dockerfile` | Runs as root, no `USER`. Sequence after L10, or the app will not boot. **[D]** |
| L12 | `Dockerfile:30` | Default **sync** workers; all three sibling Flask services use `--threads 8 --worker-class gthread`. Each cache miss blocks a whole worker. Also `get_db_conn()` omits `check_same_thread=False`, which siblings set — so adding `--threads` later will raise cross-thread errors. **[D]** |
| L13 | `requirements.txt:2` | `gunicorn` unpinned while everything else is pinned. (Weaker convention than it appears — only `powerbi_refresher` pins it; two other siblings do not.) `Flask==3.0.0` and `pyodbc==4.0.39` are both well behind current; check advisories rather than assuming. **[S]** |
| L14 | `app.py:74` | `TrustServerCertificate=yes`. Driver 18 defaults `Encrypt=yes`, so traffic *is* encrypted — what is lost is authentication of the endpoint. Needs a foothold on the path to exploit, hence low. **[D]** |
| L15 | `app.py:74` | Connection string built by f-string. No SQL injection (the query is a static literal and SQLite writes are parameterised), but ODBC treats `;` as a separator — a rotated password containing punctuation would truncate or inject attributes. | 
| L16 | `app.py:74` | No validation of `SQL_SERVER`/`SQL_USERNAME_BILOGIN`/`SQL_PASSWORD_BILOGIN`. A missing variable yields `SERVER=None;UID=None` and presents only as a per-request ODBC error. **[S]** |
| L17 | `app.py:1, 117, 123, 134`; `Dockerfile:13`; `docid.html:78, 81, 136` | Housekeeping: `import os, json, sqlite3, pyodbc` on one line; no type hints; view named `docid()`; `msg` assigned and never read at line 134 (the failure reason is discarded on the GET path); `curl` without `-fsSL` at `Dockerfile:13` writes an error body into the apt sources list; unnamed magic numbers (300ms, 550px, 50); `pagination: "local"` is the v4 form (works by accident in 6.2.1). **[S]** |

---

## 5. Suggested sequence

1. **C1** — restore the service. ~15 lines, mechanical.
2. **H5 + L8** — make the next failure visible before anything else changes.
3. **H2** — six tests plus the CI gate. Tests 1 and 2 alone would have blocked `abaa21e`.
4. **C2 + H1 + H6 together** — these are one fix, sequenced: stop coercing IDs, dedupe on the full tuple, refuse to persist an empty result. Any one alone lets the other two reintroduce the same corruption. Needs your decisions first.
5. **H3, H4, M1–M8** — resilience and observability.
6. **H8, M9, M11, M12** — packaging and exposure. Most need edits outside `docid_tool`.
7. **L1–L17** — one housekeeping commit, and rewrite the README last so it documents reality.

Items 1–3 are roughly an hour and clear the entire critical tier.

## 6. Checked and found NOT to be defects

Recorded so they are not re-investigated.

- **No XSS.** `select.innerHTML` at `docid.html:155` interpolates only the hardcoded element id, never DB data. Options use `createElement` + `.textContent`. Tabulator columns declare no formatter, so the default plaintext formatter applies. `render_template` passes no variables. (Adding `formatter: "html"` to any column later would change this.)
- **`table.setData()` with no arguments is correct** — verified against the Tabulator 6.2.1 source; it re-requests the configured `ajaxURL`. The only defect on that line is the missing `await` (L7).
- **ID columns sort numerically,** not lexicographically — Tabulator samples the first row and picks the number sorter. (Would break if a non-numeric placeholder were introduced — another argument for `null` over `0`.)
- **No SQL injection.** The query is a static literal with no user input; SQLite writes are parameterised.
- **Debug mode is off.** `app.run()` is `__main__`-guarded and is not the production path; no `FLASK_DEBUG`. Confirmed live — the 500 returns Flask's generic page with no traceback or console.
- **Dockerfile layer ordering is correct** — `COPY requirements.txt` → `pip install` → `COPY . .` caches properly.
- **Port mapping is consistent** across `EXPOSE`, `app.run`, gunicorn `--bind`, compose and README.
- **`load_dotenv()` resolves correctly** — CWD is `/app` and compose mounts `.env` to `/app/.env:ro`.
- **`combo` at `app.py:91` is used** (lines 94–95) — not dead.
- **Single-file layout at 151 lines is appropriate.** The repo's own precedent is that splitting happened at ~560 lines. No action.
- **Inline `<script>`/`<style>` matches house style** — every template in the repo does this.

## 7. Notes on scope

Several fixes need changes outside `docid_tool/` and were deliberately not pursued: the CI test gate and its placement (`.github/workflows/deploy.yml`), the gitignore pattern and the cache volume mount (`.gitignore`, `docker-compose.yml`), and the network-exposure and `.env`-scoping items, which apply to every service in the compose file rather than this one.

Two observations from outside the directory, for your awareness only:

- `sales_report_extraction/data/error_tracking/dataops_tracking.db` — a live SQLite database — is currently tracked in git, via the same gitignore gap described in H8.
- `/opt/prefect/prod/code` is simultaneously the production deployment directory and this git working tree. A push to `main` triggers `git clean -fd` and `git reset --hard origin/main` here, which would discard any uncommitted work in progress.
