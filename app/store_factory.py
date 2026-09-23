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
    """Create the storage backend chosen by the database URL.

    Resolution order for the URL: the ``database_url`` argument if given,
    otherwise the ``DATABASE_URL`` environment variable, otherwise
    ``"mdm.db"`` (a SQLite file in the current directory). Code that never
    set ``DATABASE_URL`` keeps working as before.

    URLs starting with ``postgresql://`` or ``postgres://`` get a
    ``PostgresStore``; anything else is treated as a SQLite file path and
    gets a ``Store``. Both have the same methods, so callers don't need to
    know which one they have. The backend module is imported only when
    needed, so ``psycopg2`` is not required for SQLite.

    Args:
        database_url: Optional explicit URL or file path that overrides the
            environment variable.

    Returns:
        Store | PostgresStore: A ready-to-use store with its schema created.

    Raises:
        ImportError: If a Postgres URL is used and ``psycopg2`` isn't installed.
    """
    url = database_url if database_url is not None else os.environ.get("DATABASE_URL", "mdm.db")

    if url.startswith("postgresql://") or url.startswith("postgres://"):
        from .db_postgres import PostgresStore
        return PostgresStore(url)

    from .db import Store
    return Store(url)
