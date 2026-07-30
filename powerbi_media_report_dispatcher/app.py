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

The only thing you edit regularly is SHOWS_CONFIG in config.py.

--- Where does X live? ------------------------------------------------------
  config.py            settings + SHOWS_CONFIG (the file you edit)
  state.py             SQLite: locks, logs, dispatch history
  services/auth.py     Azure AD tokens
  services/bigquery.py spend / revenue / ROAS
  services/powerbi.py  report export + dataset refresh
  services/email.py    email HTML + MS Graph send
  pipeline.py          the dispatch/refresh jobs + shared SSE plumbing
  routes.py            the HTTP endpoints (Blueprint)
  app.py               this file — builds the app, wires it together

This module stays thin on purpose: gunicorn imports `app` from here
(`app:app`), so the only jobs here are creating the Flask app, initialising
the database, and registering the routes.

Running it directly (`python app.py`) is for local dev only, and debug mode is
off unless you switch it on with FLASK_DEBUG=1 — see the bottom of this file.
"""

import os

from flask import Flask

from routes import bp
from state import init_db


def create_app() -> Flask:
    app = Flask(__name__)
    init_db()  # create tables and clear any locks left over from a crash / restart
    app.register_blueprint(bp)
    return app


# gunicorn runs `app:app` (see Dockerfile), so `app` must exist at module level.
app = create_app()


if __name__ == "__main__":
    # Local dev only. In the container gunicorn runs this (see Dockerfile).
    # Debug mode gives anyone who can reach the port a Python console, so it is
    # off unless you ask for it:  FLASK_DEBUG=1 python app.py
    # For the same reason we listen on localhost only, not 0.0.0.0.
    debug = os.environ.get("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    app.run(host="127.0.0.1", port=8002, debug=debug)
