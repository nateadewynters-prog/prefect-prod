"""Shared fixtures.

Two things must happen before `app` is imported:

1. DOCID_CACHE_DB must point at a temp file — init_db() runs at import time and
   would otherwise create docid_cache.db in the source tree.
2. pyodbc must be stubbed — it is a C extension needing unixODBC, which is not
   installed on a bare CI runner, so a real import fails before any test runs.
"""
import importlib
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class FakeOdbcError(Exception):
    """Stands in for pyodbc.Error so `except pyodbc.Error` behaves normally."""


def _install_pyodbc_stub():
    stub = MagicMock()
    stub.Error = FakeOdbcError
    sys.modules["pyodbc"] = stub
    return stub


@pytest.fixture
def app_module(tmp_path, monkeypatch):
    """Import app.py fresh, against a throwaway SQLite file."""
    monkeypatch.setenv("DOCID_CACHE_DB", str(tmp_path / "cache.db"))
    monkeypatch.setenv("SQL_SERVER", "test-server")
    monkeypatch.setenv("SQL_USERNAME_BILOGIN", "test-user")
    monkeypatch.setenv("SQL_PASSWORD_BILOGIN", "test-pass")

    _install_pyodbc_stub()
    sys.modules.pop("app", None)
    module = importlib.import_module("app")
    module.app.config.update(TESTING=True)
    return module


@pytest.fixture
def client(app_module):
    return app_module.app.test_client()


@pytest.fixture
def fake_sql(app_module):
    """Return a callable that makes the mocked SQL connection yield `rows`."""
    def _configure(rows, error=None):
        if error is not None:
            app_module.pyodbc.connect.side_effect = error
            return

        cursor = MagicMock()
        cursor.fetchall.return_value = rows

        conn = MagicMock()
        conn.cursor.return_value = cursor

        # pyodbc.connect(...) is used as a context manager.
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        ctx.__exit__.return_value = False

        app_module.pyodbc.connect.side_effect = None
        app_module.pyodbc.connect.return_value = ctx

    return _configure


ROW_A = ("Hamilton", "Victoria Palace", "Contract", 1, 10, 100)
ROW_B = ("Mamma Mia!", "Novello", "Rider", 2, 20, 200)
