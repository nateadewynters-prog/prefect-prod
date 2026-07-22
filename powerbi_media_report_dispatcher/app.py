"""
Power BI Media Report Dispatcher
================================
A small Flask tool that, for each show, will:

  1. Authenticate to Azure AD  (Power BI + MS Graph)
  2. Pull last week's spend / revenue / ROAS from BigQuery
  3. Export the show's Power BI report as PPTX  (+ a PNG preview from PDF)
  4. Email the report to that show's recipient list via MS Graph

It is deliberately built to mirror the sales_report_dispatcher so it behaves
the same and is debugged the same way: one config list, a global lock so only
one export runs at a time, live SSE logs, and a dispatch history.

The only thing you edit regularly is SHOWS_CONFIG below.
"""

import os
import time
import json
import base64
import sqlite3
from datetime import datetime, timedelta

import requests
import msal
import fitz  # PyMuPDF, for turning the PDF export into a PNG preview
from google.cloud import bigquery
from flask import Flask, render_template, Response, stream_with_context, jsonify

from dotenv import load_dotenv

load_dotenv()  # in the container this reads /app/.env (mounted read-only)
app = Flask(__name__)

# ---------------------------------------------------------------------------
# SECRETS  — everything comes from the shared .env / environment.
# Nothing sensitive is hard-coded.
# ---------------------------------------------------------------------------
TENANT_ID = os.getenv("AZURE_TENANT_ID") or os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID") or os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")
SENDER_EMAIL = os.getenv("BUSINESS_INTELLIGENCE_INBOX_ADDRESS", "figures@dewynters.com")

# BigQuery. GOOGLE_APPLICATION_CREDENTIALS must point at the mounted
# service-account JSON (set in docker-compose). We never hard-code a laptop path.
PROJECT_ID = os.getenv("GBQ_PROJECT_ID", "dewynters")
GBQ_TABLE = ("dewynters.GBQ_Dewynters_MAD_Dashboard_All_Shows"
             ".GBQ_Dewynters_MAD_Dashboard_All_Shows")

# ---------------------------------------------------------------------------
# CONFIGURATION  — one entry per show.
# ---------------------------------------------------------------------------
BASE_RECIPIENTS = [
    "a.cameron@dewynters.com", "f.joseph@dewynters.com", "c.dobson@dewynters.com",
    "h.brown@dewynters.com", "s.netherton@dewynters.com", "b.hurring@dewynters.com",
    "a.kirkham@dewynters.com", "a.conran@dewynters.com",
]

SHOWS_CONFIG = [
    {
        "id": "1", "code": "MNT", "show_name": "My Neighbour Totoro",
        "gbq_name": "My Neighbour Totoro",
        "workspace_id": "2d12753e-740c-421c-b84c-20790dedc4f2",
        "report_id": "41bfcfec-dba2-490b-9303-379fe1ed4d4c",
        "dashboard_url": "https://app.powerbi.com/groups/2d12753e-740c-421c-b84c-20790dedc4f2/reports/41bfcfec-dba2-490b-9303-379fe1ed4d4c/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS,
    },
    {
        "id": "2", "code": "BJ", "show_name": "Beetlejuice",
        "gbq_name": "Beetlejuice",
        "workspace_id": "9fe3b075-b754-4763-983e-655771e0b7c4",
        "report_id": "66d7367e-3ef8-4752-8df3-58fb40da20bd",
        "dashboard_url": "https://app.powerbi.com/groups/9fe3b075-b754-4763-983e-655771e0b7c4/reports/66d7367e-3ef8-4752-8df3-58fb40da20bd/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS + ["l.thorpe@dewynters.com", "b.jefferis@dewynters.com"],
    },
    {
        "id": "3", "code": "DWP", "show_name": "The Devil Wears Prada",
        "gbq_name": "The Devil Wears Prada",
        # TODO: not configured yet — the card is disabled until these are filled in.
        "workspace_id": "",
        "report_id": "",
        "dashboard_url": "",
        "recipients": BASE_RECIPIENTS,
    },
    {
        "id": "4", "code": "FRA", "show_name": "Frameless",
        "gbq_name": "Frameless",
        "workspace_id": "26b77406-bb46-4a03-a8bd-416757804e59",
        "report_id": "03e8cf8c-2cbf-4aaf-bb74-0fc73687307a",
        "dashboard_url": "https://app.powerbi.com/groups/26b77406-bb46-4a03-a8bd-416757804e59/reports/03e8cf8c-2cbf-4aaf-bb74-0fc73687307a/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS + ["f.carpenter@dewynters.com"],
    },
    {
        "id": "5", "code": "MML", "show_name": "Magic Mike Live",
        "gbq_name": "Magic Mike Live",
        "workspace_id": "67ad38b6-3981-401a-9032-2d0807b5f8d6",
        "report_id": "051c3e87-5bdf-4dda-8885-7103201d9a67",
        "dashboard_url": "https://app.powerbi.com/groups/67ad38b6-3981-401a-9032-2d0807b5f8d6/reports/051c3e87-5bdf-4dda-8885-7103201d9a67/2af7b29d9a85004eabc5?experience=power-bi",
        "recipients": BASE_RECIPIENTS + ["k.eastham@dewynters.com", "f.carpenter@dewynters.com"],
    },
]

# The fixed pipeline. The `id` values must match the `stage` tags used below
# and the STAGES list in dispatcher.html.
STAGES = ["auth", "bigquery", "export_pptx", "export_png", "email"]


def get_config(show_id):
    return next((s for s in SHOWS_CONFIG if s["id"] == show_id), None)


def is_configured(config):
    """A show can only be dispatched once its Power BI IDs are filled in."""
    return bool(config["workspace_id"] and config["report_id"])


# ---------------------------------------------------------------------------
# SHARED STATE DATABASE  (locks / logs / history)
# Same idea as the sales dispatcher, but a clean schema so there are no
# migrations to reason about.
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "dispatcher_state.db")


def get_db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS locks "
                     "(show_id TEXT PRIMARY KEY, is_locked INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS logs "
                     "(id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT, type TEXT, "
                     "stage TEXT, timestamp DATETIME DEFAULT (datetime('now','localtime')))")
        conn.execute("CREATE TABLE IF NOT EXISTS dispatch_history "
                     "(id INTEGER PRIMARY KEY AUTOINCREMENT, show_name TEXT, "
                     "date_range TEXT, spend REAL, revenue REAL, roas REAL, "
                     "duration_secs INTEGER, pptx_size_mb REAL, "
                     "timestamp DATETIME DEFAULT (datetime('now','localtime')))")
        # On boot, clear any locks left over from a crash / restart.
        conn.execute("UPDATE locks SET is_locked = 0")


init_db()


def set_lock(show_id, locked):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO locks (show_id, is_locked) VALUES (?, ?) "
                     "ON CONFLICT(show_id) DO UPDATE SET is_locked = ?",
                     (show_id, int(locked), int(locked)))


def is_any_locked():
    with get_db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM locks WHERE is_locked = 1").fetchone()
        return row["n"] > 0


def db_log(msg, msg_type="info", stage=None):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO logs (msg, type, stage) VALUES (?, ?, ?)",
                     (msg, msg_type, stage))


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def get_date_range():
    """Last week's Monday–Sunday, e.g. 'Monday 7th July - Sunday 13th July'."""
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)

    def with_suffix(d):
        day = d.day
        suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
        return d.strftime(f"%A {day}{suffix} %B")

    return f"{with_suffix(last_monday)} - {with_suffix(last_sunday)}"


def get_token(scopes):
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app_ = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority,
                                              client_credential=CLIENT_SECRET)
    return app_.acquire_token_for_client(scopes=scopes).get("access_token")


def get_gbq_metrics(gbq_name):
    """Last week's spend / revenue / ROAS for one show, with a source breakdown.

    Uses a parameterised query so the show name can't break the SQL.
    """
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT MAD_Media_Source AS source,
               SUM(MAD_All_Spend)   AS spend,
               SUM(MAD_All_Revenue) AS revenue
        FROM `{GBQ_TABLE}`
        WHERE MAD_Show_Name = @show
          AND MAD_Media_Source IN ('Meta', 'Google Ads', 'Programmatic Spend',
                                    'Programmatic', 'TikTok')
          AND Date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 WEEK)
          AND Date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)
        GROUP BY MAD_Media_Source
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("show", "STRING", gbq_name)
    ])
    rows = list(client.query(query, job_config=job_config).result())

    breakdown = [{"source": r.source, "spend": r.spend or 0.0, "revenue": r.revenue or 0.0}
                 for r in rows]
    spend = sum(b["spend"] for b in breakdown)
    revenue = sum(b["revenue"] for b in breakdown)
    roas = (revenue / spend) if spend > 0 else 0.0
    return {"spend": spend, "revenue": revenue, "roas": roas, "breakdown": breakdown}


def powerbi_export(pbi_token, workspace_id, report_id, fmt, log=None):
    """Trigger a Power BI 'ExportTo', poll until done, return the raw file bytes.

    fmt is 'PPTX' or 'PDF'. Raises on failure.
    """
    headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
    base = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}"

    resp = requests.post(f"{base}/ExportTo", headers=headers, json={"format": fmt})
    resp.raise_for_status()
    export_id = resp.json()["id"]

    status_url = f"{base}/exports/{export_id}"
    while True:
        time.sleep(5)
        status = requests.get(status_url, headers=headers).json().get("status")
        if log:
            log(f"⏳ {fmt} export: {status}...")
        if status == "Succeeded":
            break
        if status == "Failed":
            raise RuntimeError(f"Power BI {fmt} export failed")

    return requests.get(f"{status_url}/file", headers=headers).content


def pdf_first_page_to_png(pdf_bytes):
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pix = doc.load_page(0).get_pixmap(matrix=fitz.Matrix(2, 2))
    png_bytes = pix.tobytes("png")
    doc.close()
    return png_bytes


def build_email_html(config, metrics, date_range):
    # NOTE: ROAS is a ratio (revenue ÷ spend). We show it with a £ sign to match
    # the house style the recipients are used to from the original scripts,
    # i.e. "£3.42" reads as "£3.42 back per £1 spent".
    return f"""
    <html>
      <body style="font-family: Calibri, sans-serif; font-size: 11pt; color: #000000;">
        <p>Dear All,</p>
        <p>Please find attached your weekly digital media report for {config['show_name']}.</p>
        <p>You can find a link to the dashboard <a href="{config['dashboard_url']}">here</a>.</p>
        <p><b>{date_range}</b></p>
        <ul style="list-style-type: disc; margin-top: 0; margin-bottom: 0;">
          <li>Total Spend: &pound;{metrics['spend']:,.2f}</li>
          <li>Total Revenue: &pound;{metrics['revenue']:,.2f}</li>
          <li>Overall ROAS: &pound;{metrics['roas']:.2f}</li>
        </ul>
        <p>All the best,<br>The Dewynters Team</p>
        <br>
        <img src="cid:report_image" style="width: 100%; max-width: 800px; border: 1px solid #ccc;">
      </body>
    </html>
    """


def send_graph_email(config, html_body, pptx_bytes, png_bytes, date_range, graph_token):
    payload = {
        "message": {
            "subject": f"{config['show_name']} - Digital Media Report - {date_range}",
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": e}} for e in config["recipients"]],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": f"{config['code']}_Digital_Media_Report_{datetime.now():%Y%m%d}.pptx",
                    "contentType": "application/vnd.openxmlformats-officedocument."
                                   "presentationml.presentation",
                    "contentBytes": base64.b64encode(pptx_bytes).decode(),
                    "isInline": False,
                },
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "Preview.png",
                    "contentType": "image/png",
                    "contentBytes": base64.b64encode(png_bytes).decode(),
                    "contentId": "report_image",
                    "isInline": True,
                },
            ],
        }
    }
    url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    headers = {"Authorization": f"Bearer {graph_token}", "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()  # Graph returns 202 on success


# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------
@app.route("/")
def dispatcher():
    return render_template("dispatcher.html", shows=SHOWS_CONFIG)


@app.route("/api/history")
def api_history():
    with get_db_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM dispatch_history").fetchone()[0]
        history = [dict(row) for row in conn.execute(
            "SELECT show_name, date_range, spend, revenue, roas, duration_secs, "
            "pptx_size_mb, timestamp FROM dispatch_history "
            "ORDER BY timestamp DESC LIMIT 50").fetchall()]
    return jsonify({"total": total, "history": history})


@app.route("/api/state")
def api_state():
    with get_db_conn() as conn:
        locks = [r["show_id"] for r in
                 conn.execute("SELECT show_id FROM locks WHERE is_locked = 1").fetchall()]
        cutoff = (datetime.now() - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        logs = [dict(r) for r in conn.execute(
            "SELECT msg, type, stage, timestamp FROM logs WHERE timestamp >= ? "
            "ORDER BY timestamp ASC", (cutoff,)).fetchall()]
    return jsonify({"locks": locks, "logs": logs})


@app.route("/metrics/<show_id>")
def api_metrics(show_id):
    """Powers the inline 'Media Figures' panel — one BigQuery read per click."""
    config = get_config(show_id)
    if not config:
        return {"error": "Show not found"}, 404
    try:
        m = get_gbq_metrics(config["gbq_name"])
        rows = [{"Metric": b["source"],
                 "Value": f"£{b['spend']:,.0f} spend · £{b['revenue']:,.0f} rev"}
                for b in m["breakdown"]]
        rows.append({"Metric": "Total Spend", "Value": f"£{m['spend']:,.2f}"})
        rows.append({"Metric": "Total Revenue", "Value": f"£{m['revenue']:,.2f}"})
        rows.append({"Metric": "Overall ROAS", "Value": f"£{m['roas']:.2f}"})
        return {"show": config["show_name"], "data": rows}
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/preview/<show_id>")
def preview_email(show_id):
    config = get_config(show_id)
    if not config:
        return "Show not found", 404
    try:
        metrics = get_gbq_metrics(config["gbq_name"])
        return build_email_html(config, metrics, get_date_range())
    except Exception as e:
        return f"Error building preview: {str(e)}", 500


@app.route("/stream/<show_id>")
def stream_logs(show_id):
    # Global lock: if ANY show is dispatching, refuse. Power BI limits how many
    # exports can run at once, so we keep it to one at a time.
    if is_any_locked():
        def reject():
            yield ('data: ' + json.dumps({
                "msg": "❌ System busy: another report is being processed. Please wait.",
                "type": "error"}) + "\n\n")
            yield "data: [DONE]\n\n"
        return Response(stream_with_context(reject()), mimetype="text/event-stream")

    set_lock(show_id, True)

    def generate():
        current_stage = {"name": None}

        def msg(text, msg_type="info", stage=None):
            s = stage if stage is not None else current_stage["name"]
            db_log(text, msg_type, s)
            return "data: " + json.dumps({"msg": text, "type": msg_type, "stage": s}) + "\n\n"

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
            pbi_token = get_token(["https://analysis.windows.net/powerbi/api/.default"])
            graph_token = get_token(["https://graph.microsoft.com/.default"])
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
            with get_db_conn() as conn:
                conn.execute(
                    "INSERT INTO dispatch_history (show_name, date_range, spend, revenue, "
                    "roas, duration_secs, pptx_size_mb) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (config["show_name"], date_range, metrics["spend"], metrics["revenue"],
                     metrics["roas"], duration_secs, pptx_size_mb))
                conn.commit()

            yield "data: [DONE]\n\n"

        finally:
            set_lock(show_id, False)

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


@app.route("/refresh/<show_id>")
def refresh_dataset(show_id):
    """Trigger a Power BI dataset refresh for one show (no export, no email).

    The dataset ID isn't stored in SHOWS_CONFIG — we look it up from the report,
    since every report knows the dataset it's built on. Uses the same global
    lock as a dispatch, so the two can't run at the same time.
    """
    if is_any_locked():
        def reject():
            yield ("data: " + json.dumps({
                "msg": "❌ System busy: another job is running. Please wait.",
                "type": "error"}) + "\n\n")
            yield "data: [DONE]\n\n"
        return Response(stream_with_context(reject()), mimetype="text/event-stream")

    set_lock(show_id, True)

    def generate():
        current_stage = {"name": None}

        def msg(text, msg_type="info", stage=None):
            s = stage if stage is not None else current_stage["name"]
            db_log(text, msg_type, s)
            return "data: " + json.dumps({"msg": text, "type": msg_type, "stage": s}) + "\n\n"

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
            pbi_token = get_token(["https://analysis.windows.net/powerbi/api/.default"])
            if not pbi_token:
                yield msg("❌ Auth failed. Check Azure AD credentials.", "error")
                return
            headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
            base = f"https://api.powerbi.com/v1.0/myorg/groups/{config['workspace_id']}"

            # 2. REFRESH ------------------------------------------------------
            current_stage["name"] = "refresh"
            yield msg("🔎 Finding the report's dataset...")
            try:
                report = requests.get(f"{base}/reports/{config['report_id']}", headers=headers)
                report.raise_for_status()
                dataset_id = report.json().get("datasetId")
            except Exception as e:
                yield msg(f"❌ Could not read report: {str(e)}", "error")
                return
            if not dataset_id:
                yield msg("❌ No dataset linked to this report.", "error")
                return

            yield msg("🔄 Triggering dataset refresh...")
            refresh_url = f"{base}/datasets/{dataset_id}/refreshes"
            status_url = f"{base}/datasets/{dataset_id}/refreshes?$top=1"
            try:
                trigger = requests.post(refresh_url, headers=headers, json={})
                body = trigger.text.lower()
                # If a scheduled refresh is already running, attach to it rather than error.
                if trigger.status_code == 400 and ("already executing" in body
                                                   or "refreshinprogress" in body):
                    yield msg("ℹ️ A refresh is already running. Attaching to it...")
                else:
                    trigger.raise_for_status()

                while True:
                    poll = requests.get(status_url, headers=headers)
                    poll.raise_for_status()
                    status = poll.json().get("value", [{}])[0].get("status", "Unknown")
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

            yield "data: [DONE]\n\n"

        finally:
            set_lock(show_id, False)

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


if __name__ == "__main__":
    # Local dev only. In the container gunicorn runs this (see Dockerfile).
    app.run(host="0.0.0.0", port=8002, debug=True)