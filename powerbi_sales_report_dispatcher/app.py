"""
Power BI Sales Report Dispatcher
================================
A small Flask tool that, for each show, will:

  1. Authenticate to Azure AD  (Power BI + MS Graph)
  2. Refresh the show's Power BI dataset and wait for it to finish
  3. Pull yesterday's wrap / advance / cumulative figures from SQL Server
  4. Export the show's Power BI report as PDF  (+ a PNG preview of page 1)
  5. Email the report to that show's recipient list via MS Graph

It mirrors the powerbi_media_report_dispatcher so both tools behave the same
and are debugged the same way: one config list, a global lock so only one
export runs at a time, live SSE logs, and a dispatch history.

The only thing you edit regularly is SHOWS_CONFIG in config.py.

--- Where does X live? ------------------------------------------------------
  config.py            settings + SHOWS_CONFIG (the file you edit)
  state.py             SQLite: locks, logs, dispatch history
  services/auth.py     Azure AD tokens
  services/sql.py      sales metrics (Legacy / TransactLive)
  services/powerbi.py  dataset refresh + report export + PDF->PNG
  services/email.py    email HTML + MS Graph send
  pipeline.py          the dispatch job + shared SSE plumbing
  routes.py            the HTTP endpoints (Blueprint)
  app.py               this file — builds the app, wires it together

This module stays thin on purpose: gunicorn imports `app` from here
(`app:app`), so the only jobs here are creating the Flask app, initialising
the database, and registering the routes.
"""

from flask import Flask

from routes import bp
from state import init_db


def create_app() -> Flask:
    app = Flask(__name__)
    init_db()  # create tables, run the column migrations, clear stale locks
    app.register_blueprint(bp)
    return app


# gunicorn runs `app:app` (see Dockerfile), so `app` must exist at module level.
app = create_app()


if __name__ == '__main__':
    # Local dev only. In the container gunicorn runs this (see Dockerfile).
    app.run(host='0.0.0.0', port=8002)
