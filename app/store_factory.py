"""
Picks the storage backend based on DATABASE_URL:

  - unset, or a plain path        -> SQLite (db.py's `Store`) -- fully
                                      tested, zero external dependencies,
                                      the default so nothing changes for
                                      existing usage.
  - postgresql://... or postgres://... -> Postgres (db_postgres.py's
                                      `PostgresStore`) -- see that module's
                                      docstring for the "untested in this
                                      sandbox" caveat.

Both classes implement the identical method signatures (same tenant_id +
domain scoping pattern), so `pipeline.py` and `api.py` never need to know
which one they're talking to -- they just call `get_store()`.
"""
from __future__ import annotations

import os


def get_store(database_url: str | None = None):
    """database_url: explicit override, else reads the DATABASE_URL env
    var, else falls back to the SQLite default ("mdm.db" in the current
    directory) -- so existing code that never set DATABASE_URL keeps
    working exactly as before."""
    url = database_url if database_url is not None else os.environ.get("DATABASE_URL", "mdm.db")

    if url.startswith("postgresql://") or url.startswith("postgres://"):
        from .db_postgres import PostgresStore
        return PostgresStore(url)

    from .db import Store
    return Store(url)
