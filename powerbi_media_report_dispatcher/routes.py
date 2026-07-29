"""
routes.py — all the HTTP endpoints, as a Flask Blueprint.

Kept deliberately thin: each handler validates input, calls a helper
(state.py for data, pipeline.py for the streamed jobs, the services for
one-off reads), and returns. The exact routes, methods and JSON shapes here
are a contract with dispatcher.html — don't rename a route or change a key
without updating the template.

  GET /                board UI
  GET /api/history     {"total": N, "history": [...]}
  GET /api/state       {"locks": [...], "logs": [...]}
  GET /metrics/<id>    {"show": ..., "data": [{Metric, Value}, ...]}  (or {error})
  GET /preview/<id>    raw email HTML
  GET /stream/<id>     SSE: run a dispatch
  GET /refresh/<id>    SSE: run a dataset refresh
"""

from flask import Blueprint, render_template, jsonify

from config import SHOWS_CONFIG, get_config
from state import get_active_locks, get_recent_logs, get_history
from services.bigquery import get_gbq_metrics
from services.email import build_email_html
from pipeline import guarded_stream, get_date_range

bp = Blueprint("routes", __name__)


@bp.route("/")
def dispatcher():
    return render_template("dispatcher.html", shows=SHOWS_CONFIG)


@bp.route("/api/history")
def api_history():
    return jsonify(get_history())


@bp.route("/api/state")
def api_state():
    return jsonify({"locks": get_active_locks(), "logs": get_recent_logs()})


@bp.route("/metrics/<show_id>")
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


@bp.route("/preview/<show_id>")
def preview_email(show_id):
    config = get_config(show_id)
    if not config:
        return "Show not found", 404
    try:
        metrics = get_gbq_metrics(config["gbq_name"])
        return build_email_html(config, metrics, get_date_range())
    except Exception as e:
        return f"Error building preview: {str(e)}", 500


@bp.route("/stream/<show_id>")
def stream_logs(show_id):
    # Global lock lives in pipeline.guarded_stream: refuse if a job is running,
    # otherwise take the lock and stream the dispatch.
    return guarded_stream(show_id, "dispatch")


@bp.route("/refresh/<show_id>")
def refresh_dataset(show_id):
    return guarded_stream(show_id, "refresh")
