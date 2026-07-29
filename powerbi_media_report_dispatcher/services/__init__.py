"""
services/ — one module per external system the dispatcher talks to.

  auth.py      Azure AD tokens (MSAL)
  bigquery.py  last week's spend / revenue / ROAS
  powerbi.py   report export (PPTX/PDF→PNG) and dataset refresh
  email.py     building and sending the report email via MS Graph

Each module is self-contained: it reads its settings from config.py and
knows nothing about Flask, SSE, or the database.
"""
