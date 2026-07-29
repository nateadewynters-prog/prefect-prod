"""
pipeline.py — the step-by-step jobs, streamed live to the browser as SSE.

Two jobs live here:
  run_dispatch  auth → BigQuery → export PPTX → export PNG → email
  run_refresh   auth → trigger a Power BI dataset refresh

Both are generator functions: they `yield` one Server-Sent Event per step so
the UI can show progress in real time. The SSE plumbing that used to be
copy-pasted into both routes is factored out here:

  * SSE_DONE            the terminating event every stream must end with
  * _stage_logger()     builds the `msg()` helper (logs to the DB *and*
                        returns the formatted SSE line), tracking the current
                        stage so messages get tagged with the right stage id
  * guarded_stream()    the global-lock gate: refuses when a job is already
                        running, otherwise sets the lock, streams the job, and
                        releases the lock in a finally block

Stage tags ("auth", "bigquery", ...) MUST match the ids in dispatcher.html —
the front-end maps them to the progress bar. Don't rename them here without
updating the template.
"""

import json
import time
from datetime import datetime, timedelta

import requests
from flask import Response, stream_with_context

from config import get_config, is_configured
from state import set_lock, is_any_locked, db_log, record_dispatch
from services.auth import get_token, POWERBI_SCOPES, GRAPH_SCOPES
from services.bigquery import get_gbq_metrics
from services.powerbi import (powerbi_export, pdf_first_page_to_png,
                              find_dataset_id, trigger_refresh, poll_refresh_status)
from services.email import build_email_html, send_graph_email

# Every SSE stream ends with exactly this line; the front-end closes the
# EventSource when it sees "[DONE]".
SSE_DONE = "data: [DONE]\n\n"


def get_date_range() -> str:
    """Last week's Monday–Sunday, e.g. 'Monday 7th July - Sunday 13th July'."""
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)

    def with_suffix(d):
        day = d.day
        suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
        return d.strftime(f"%A {day}{suffix} %B")

    return f"{with_suffix(last_monday)} - {with_suffix(last_sunday)}"


def _sse(payload: dict) -> str:
    """Format one Server-Sent Event: literally `data: {json}\\n\\n`."""
    return "data: " + json.dumps(payload) + "\n\n"


def _stage_logger():
    """Build the per-run `msg()` helper shared by both jobs.

    `msg(text, ...)` writes the line to the DB (so /api/state can replay it)
    AND returns the SSE line to yield. It defaults each message's stage to
    whatever `current_stage["name"]` is, so a job just sets the stage once at
    the top of each step. Returns (current_stage, msg).
    """
    current_stage = {"name": None}

    def msg(text: str, msg_type: str = "info", stage: str | None = None) -> str:
        s = stage if stage is not None else current_stage["name"]
        db_log(text, msg_type, s)
        return _sse({"msg": text, "type": msg_type, "stage": s})

    return current_stage, msg


def _reject(message: str):
    """The 'system busy' stream: one error event, then DONE. No stage tag —
    matches the original payload the UI expects."""
    def gen():
        yield _sse({"msg": message, "type": "error"})
        yield SSE_DONE
    return gen()


# Busy messages differ per job, kept verbatim so the UI reads the same text.
_BUSY_MESSAGE = {
    "dispatch": "❌ System busy: another report is being processed. Please wait.",
    "refresh": "❌ System busy: another job is running. Please wait.",
}


def guarded_stream(show_id: str, kind: str) -> Response:
    """Global lock + streaming, shared by /stream and /refresh.

    If ANY show is busy, refuse (Power BI limits concurrent jobs, so we keep
    it to one at a time). Otherwise take the lock and stream the job; the job
    generator releases the lock in its own finally block.
    """
    if is_any_locked():
        return Response(stream_with_context(_reject(_BUSY_MESSAGE[kind])),
                        mimetype="text/event-stream")

    set_lock(show_id, True)
    job = run_dispatch(show_id) if kind == "dispatch" else run_refresh(show_id)
    return Response(stream_with_context(job), mimetype="text/event-stream")


def run_dispatch(show_id: str):
    """Full dispatch: auth, BigQuery, PPTX + PNG export, email, record history."""
    current_stage, msg = _stage_logger()

    try:
        config = get_config(show_id)
        if not config:
            yield msg("❌ Show not found.", "error")
            return
        if not is_configured(config):
            yield msg(f"❌ {config['show_name']} is not configured yet "
                      "(missing Power BI workspace/report ID).", "error")
            return

        yield msg(f"========== NEW DISPATCH: {config['show_name'].upper()} ==========",
                  "separator")
        date_range = get_date_range()
        yield msg(f"🚀 Starting {config['show_name']} — {date_range}")

        start_time = time.time()

        # 1. AUTH ---------------------------------------------------------
        current_stage["name"] = "auth"
        yield msg("🔑 Requesting Azure AD tokens...")
        pbi_token = get_token(POWERBI_SCOPES)
        graph_token = get_token(GRAPH_SCOPES)
        if not pbi_token or not graph_token:
            yield msg("❌ Auth failed. Check Azure AD credentials.", "error")
            return

        # 2. BIGQUERY -----------------------------------------------------
        current_stage["name"] = "bigquery"
        yield msg("🛰️ Fetching spend / revenue from BigQuery...")
        try:
            metrics = get_gbq_metrics(config["gbq_name"])
        except Exception as e:
            yield msg(f"❌ BigQuery error: {str(e)}", "error")
            return
        if metrics["spend"] == 0 and metrics["revenue"] == 0:
            yield msg(f"⚠️ No spend/revenue found for {config['show_name']} last week. "
                      "Sending anyway with zeros.", "info")
        yield msg(f"📊 Spend £{metrics['spend']:,.2f} · Revenue £{metrics['revenue']:,.2f} "
                  f"· ROAS {metrics['roas']:.2f}x", "success")

        # 3. EXPORT PPTX --------------------------------------------------
        current_stage["name"] = "export_pptx"
        yield msg("📊 Exporting Power BI report (PPTX)...")
        try:
            pptx_bytes = powerbi_export(pbi_token, config["workspace_id"],
                                        config["report_id"], "PPTX",
                                        log=lambda t: db_log(t, "info", "export_pptx"))
        except Exception as e:
            yield msg(f"❌ PPTX export error: {str(e)}", "error")
            return
        yield msg("✅ PPTX export complete.", "success")

        # 4. EXPORT PNG PREVIEW (via PDF) ---------------------------------
        current_stage["name"] = "export_png"
        yield msg("🖼️ Exporting PDF and rendering PNG preview...")
        try:
            pdf_bytes = powerbi_export(pbi_token, config["workspace_id"],
                                       config["report_id"], "PDF",
                                       log=lambda t: db_log(t, "info", "export_png"))
            png_bytes = pdf_first_page_to_png(pdf_bytes)
        except Exception as e:
            yield msg(f"❌ Preview render error: {str(e)}", "error")
            return
        yield msg("✅ Preview ready.", "success")

        # 5. EMAIL --------------------------------------------------------
        current_stage["name"] = "email"
        yield msg("📧 Dispatching email via MS Graph...")
        try:
            html_body = build_email_html(config, metrics, date_range)
            send_graph_email(config, html_body, pptx_bytes, png_bytes,
                             date_range, graph_token)
        except Exception as e:
            yield msg(f"❌ Graph API error: {str(e)}", "error")
            return

        yield msg(f"✅ SUCCESS: {config['show_name']} report sent.", "success")
        yield msg("Sent to: " + ", ".join(config["recipients"]), "info")

        # Record history
        duration_secs = round(time.time() - start_time)
        pptx_size_mb = round(len(pptx_bytes) / (1024 * 1024), 2)
        record_dispatch(config["show_name"], date_range, metrics["spend"],
                        metrics["revenue"], metrics["roas"], duration_secs,
                        pptx_size_mb)

        yield SSE_DONE

    finally:
        set_lock(show_id, False)


def run_refresh(show_id: str):
    """Trigger a Power BI dataset refresh for one show (no export, no email).

    The dataset ID isn't stored in SHOWS_CONFIG — we look it up from the report,
    since every report knows the dataset it's built on. Uses the same global
    lock as a dispatch, so the two can't run at the same time.
    """
    current_stage, msg = _stage_logger()

    try:
        config = get_config(show_id)
        if not config:
            yield msg("❌ Show not found.", "error")
            return
        if not is_configured(config):
            yield msg(f"❌ {config['show_name']} is not configured yet.", "error")
            return

        yield msg(f"========== NEW REFRESH: {config['show_name'].upper()} ==========",
                  "separator")

        # 1. AUTH ---------------------------------------------------------
        current_stage["name"] = "auth"
        yield msg("🔑 Requesting Azure AD token...")
        pbi_token = get_token(POWERBI_SCOPES)
        if not pbi_token:
            yield msg("❌ Auth failed. Check Azure AD credentials.", "error")
            return

        # 2. REFRESH ------------------------------------------------------
        current_stage["name"] = "refresh"
        yield msg("🔎 Finding the report's dataset...")
        try:
            dataset_id = find_dataset_id(pbi_token, config["workspace_id"],
                                         config["report_id"])
        except Exception as e:
            yield msg(f"❌ Could not read report: {str(e)}", "error")
            return
        if not dataset_id:
            yield msg("❌ No dataset linked to this report.", "error")
            return

        yield msg("🔄 Triggering dataset refresh...")
        try:
            trigger = trigger_refresh(pbi_token, config["workspace_id"], dataset_id)
            body = trigger.text.lower()
            # If a scheduled refresh is already running, attach to it rather than error.
            if trigger.status_code == 400 and ("already executing" in body
                                               or "refreshinprogress" in body):
                yield msg("ℹ️ A refresh is already running. Attaching to it...")
            else:
                trigger.raise_for_status()

            while True:
                status = poll_refresh_status(pbi_token, config["workspace_id"], dataset_id)
                yield msg(f"⏳ Refresh status: {status}...")
                if status == "Completed":
                    yield msg(f"✅ {config['show_name']} dataset refreshed.", "success")
                    break
                if status == "Failed":
                    yield msg("❌ Power BI refresh failed.", "error")
                    return
                time.sleep(5)
        except requests.exceptions.HTTPError as e:
            yield msg(f"❌ API error: {e.response.status_code} - {e.response.text}", "error")
            return
        except Exception as e:
            yield msg(f"❌ Refresh error: {str(e)}", "error")
            return

        yield SSE_DONE

    finally:
        set_lock(show_id, False)
