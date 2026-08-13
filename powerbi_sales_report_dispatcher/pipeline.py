"""
pipeline.py — the dispatch job, plus the shared SSE plumbing around it.

The job is a generator: it yields Server-Sent Event lines as it goes, and the
same lines are written to the logs table on the way past. That is why a browser
refresh mid-run loses nothing — /api/state replays the last 30 minutes.

guarded_stream() is the lock gate. The lock is taken here, before the generator
starts, and released in the generator's finally.

Stage tags ("auth", "refresh", ...) drive the stepper in dispatcher.html and
must stay in step with config.STAGES.
"""

import json
import time

import requests
from flask import Response, stream_with_context

from config import get_config
from state import is_any_locked, set_lock, db_log, record_dispatch
from services.auth import LiveReportingEngine, POWERBI_SCOPES, GRAPH_SCOPES
from services.sql import get_show_metrics
from services.email import build_email_html, send_graph_email
from services import powerbi

SSE_DONE = "data: [DONE]\n\n"

_BUSY_MESSAGE = "❌ System busy: Another report is currently being processed. Please wait until it completes."


def _sse(payload):
    return f'data: {json.dumps(payload)}\n\n'


def _stage_logger():
    """Returns (current_stage, msg).

    current_stage is a one-key dict used as a closure cell: set
    current_stage["name"] = "sql" and every later msg() is tagged with it.
    msg() both writes the log row and returns the SSE line, so the job body
    reads `yield msg("...")`.
    """
    current_stage = {"name": None}

    def msg(text, msg_type="info", stage=None):
        s = stage if stage is not None else current_stage["name"]
        db_log(text, msg_type, s)
        return _sse({"msg": text, "type": msg_type, "stage": s})

    return current_stage, msg


def _reject(message):
    yield _sse({"msg": message, "type": "error"})
    yield SSE_DONE


def guarded_stream(show_id):
    """Refuse if any dispatch is running, otherwise take the lock and stream."""
    if is_any_locked():
        return Response(stream_with_context(_reject(_BUSY_MESSAGE)), mimetype='text/event-stream')

    set_lock(show_id, True)
    return Response(stream_with_context(run_dispatch(show_id)), mimetype='text/event-stream')


def run_dispatch(show_id):
    current_stage, msg = _stage_logger()

    try:
        config = get_config(show_id)
        if not config:
            yield msg("❌ Error: Show not found", "error")
            return

        yield msg(f"========== NEW DISPATCH: {config['show_name'].upper()} ==========", "separator")
        yield msg(f"🚀 Starting pipeline for {config['show_name']}...")

        start_time = time.time()

        # 1. AUTH ------------------------------------------------------------
        engine = LiveReportingEngine()
        current_stage["name"] = "auth"
        yield msg("🔑 Requesting Azure AD Tokens...")
        pbi_token = engine.get_token(POWERBI_SCOPES)
        graph_token = engine.get_token(GRAPH_SCOPES)

        if not pbi_token or not graph_token:
            yield msg("❌ Auth Failed. Check Azure AD Credentials.", "error")
            return

        pbi_headers = powerbi.auth_headers(pbi_token)

        # 2. DATASET REFRESH -------------------------------------------------
        current_stage["name"] = "refresh"
        yield msg("🔄 Triggering Power BI Dataset Refresh...")
        refresh_start = time.time()
        refresh_secs = None
        try:
            trigger_req = powerbi.trigger_refresh(pbi_headers, config['pbi_workspace_id'], config['pbi_dataset_id'])

            # A scheduled refresh may already be in flight — that's not an error,
            # we just attach to it. Anything else is raised.
            if trigger_req.status_code == 400 and ("already executing" in trigger_req.text.lower() or "refreshinprogress" in trigger_req.text.lower()):
                yield msg("ℹ️ A scheduled refresh is already running. Attaching to it...", "info")
            else:
                trigger_req.raise_for_status()

            while True:
                status = powerbi.get_refresh_status(pbi_headers, config['pbi_workspace_id'], config['pbi_dataset_id'])

                yield msg(f"⏳ Refresh Status: {status}...")
                if status == "Completed":
                    refresh_secs = time.time() - refresh_start
                    yield msg("✅ Dataset Refresh Completed.", "success")
                    break
                elif status == "Failed":
                    yield msg("❌ Power BI Refresh Failed.", "error")
                    return
                time.sleep(5)

        except requests.exceptions.HTTPError as e:
            yield msg(f"❌ API Error: {e.response.status_code} - {e.response.text}", "error")
            return
        except Exception as e:
            yield msg(f"❌ Refresh API Error: {str(e)}", "error")
            return

        # 3. SQL -------------------------------------------------------------
        current_stage["name"] = "sql"
        yield msg("🗄️ Fetching Sales Metrics from SQL...")
        sql_start = time.time()
        sql_secs = None
        try:
            metrics = get_show_metrics(config)

            if not metrics.get('main'):
                yield msg(f"⛔ No sales data found for {config['show_name']} — report not sent.", "error")
                return
            if not metrics.get('weekly'):
                yield msg(f"⛔ No weekly performance data found for {config['show_name']} — report not sent.", "error")
                return

            sql_secs = time.time() - sql_start
            yield msg(f"📊 SQL Data Fetched.")
        except Exception as e:
            yield msg(f"❌ SQL Error: {str(e)}", "error")
            return

        # 4. EXPORT + DOWNLOAD -----------------------------------------------
        current_stage["name"] = "export"
        yield msg("📄 Triggering Power BI PDF Export...")
        export_start = time.time()
        export_secs = None
        try:
            export_id = powerbi.start_export(pbi_headers, config['pbi_workspace_id'], config['pbi_report_id'])

            while True:
                status = powerbi.get_export_status(pbi_headers, config['pbi_workspace_id'], config['pbi_report_id'], export_id)

                yield msg(f"⏳ Export Status: {status}...")
                if status == "Succeeded":
                    break
                elif status == "Failed":
                    yield msg("❌ Power BI Export Failed.", "error")
                    return
                time.sleep(5)

            current_stage["name"] = "download"
            yield msg("📥 Downloading PDF File...")
            pdf_bytes = powerbi.download_export(pbi_headers, config['pbi_workspace_id'], config['pbi_report_id'], export_id)
            export_secs = time.time() - export_start
        except Exception as e:
            yield msg(f"❌ Export API Error: {str(e)}", "error")
            return

        # 5. RENDER PREVIEW ---------------------------------------------------
        current_stage["name"] = "render"
        yield msg("🖼️ Rendering PNG Preview from PDF...")
        try:
            png_bytes = powerbi.pdf_first_page_to_png(pdf_bytes)
        except Exception as e:
            yield msg(f"❌ Rendering Error: {str(e)}", "error")
            return

        # 6. EMAIL ------------------------------------------------------------
        current_stage["name"] = "email"
        yield msg("📧 Dispatching Email via MS Graph...")
        try:
            send_graph_email(config, build_email_html(config, metrics), pdf_bytes, png_bytes, graph_token)
            yield msg(f"✅ SUCCESS: {config['show_name']} report sent.", "success")

            email_list = ", ".join(config['recipients'])
            yield msg(f"Sent to email addresses: {email_list}", "info")

            duration_mins = max(1, round((time.time() - start_time) / 60))
            pdf_size_mb = round(len(pdf_bytes) / (1024 * 1024), 2)

            record_dispatch(config['show_name'], duration_mins, pdf_size_mb, refresh_secs, sql_secs, export_secs)

        except Exception as e:
            yield msg(f"❌ Graph API Error: {str(e)}", "error")
            return

        yield SSE_DONE

    finally:
        set_lock(show_id, False)
