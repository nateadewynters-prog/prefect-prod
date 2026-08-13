"""
services/ — one module per external system this tool talks to.

  auth.py     Azure AD tokens (Power BI + MS Graph)
  sql.py      sales metrics out of SQL Server (Legacy / TransactLive)
  powerbi.py  dataset refresh, report export, PDF -> PNG
  email.py    email HTML + MS Graph send

These modules are deliberately Flask-free: they make a call and return data or
raise. Anything to do with the live SSE log or the lock belongs in pipeline.py.
"""
