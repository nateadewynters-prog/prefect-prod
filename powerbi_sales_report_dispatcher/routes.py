"""
routes.py — all the HTTP endpoints, as a Flask Blueprint.

Kept deliberately thin: each handler validates input, calls a helper
(state.py for data, pipeline.py for the streamed job, the services for one-off
reads), and returns. The exact routes, methods and JSON shapes here are a
contract with dispatcher.html — don't rename a route or change a key without
updating the template.

  GET /                board UI
  GET /api/history     {"total": N, "history": [...]}
  GET /api/state       {"locks": [...], "logs": [...]}
  GET /preview/<id>    raw email HTML
  GET /query/<id>      {"show": ..., "data": [{Metric, Value}, ...]}  (or {error})
  GET /stream/<id>     SSE: run a dispatch
"""

from flask import Blueprint, render_template, jsonify

from config import SHOWS_CONFIG, get_config
from state import get_active_locks, get_recent_logs, get_history
from services.sql import get_show_metrics
from services.email import build_email_html
from pipeline import guarded_stream

bp = Blueprint("routes", __name__)


@bp.route('/')
def dispatcher():
    return render_template('dispatcher.html', shows=SHOWS_CONFIG)


@bp.route('/api/history')
def api_history():
    return jsonify(get_history())


@bp.route('/api/state')
def api_state():
    return jsonify({"locks": get_active_locks(), "logs": get_recent_logs()})


@bp.route('/preview/<show_id>')
def preview_email(show_id):
    config = get_config(show_id)
    if not config: return "Show not found", 404
    try:
        metrics = get_show_metrics(config)
        return build_email_html(config, metrics)
    except Exception as e:
        return f"Error fetching preview data: {str(e)}", 500


@bp.route('/query/<show_id>')
def query_database(show_id):
    config = get_config(show_id)
    if not config: return {"error": "Show not found"}, 404
    try:
        m = get_show_metrics(config)
        res = [
            {"Metric": "Wrap", "Value": f"£{m['main'][0]}"},
            {"Metric": "Tickets Sold", "Value": m['main'][1]},
            {"Metric": "ATP", "Value": f"£{m['main'][2]}"},
            {"Metric": "Advance £", "Value": f"£{m['main'][3]}"},
            {"Metric": "Advance Tix", "Value": m['main'][4]},
            {"Metric": "Reserve £", "Value": f"£{m['main'][6]}"},
            {"Metric": "Cumul Gross £", "Value": f"£{m['main'][7]}"},
            {"Metric": "Weekly GP %", "Value": f"{m['weekly'][0]}%"},
            {"Metric": "Weekly Cap %", "Value": f"{m['weekly'][1]}%"}
        ]
        return {"show": config['show_name'], "data": res}
    except Exception as e: return {"error": str(e)}, 500


@bp.route('/stream/<show_id>')
def stream_logs(show_id):
    # Global lock lives in pipeline.guarded_stream: refuse if a dispatch is
    # already running, otherwise take the lock and stream this one.
    return guarded_stream(show_id)
