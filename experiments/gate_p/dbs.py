"""
Database adapters for the Gate P measurement. All three expose the same small interface to SqlState:

    dialect                              app/sql_dialect.py's Dialect for this database
    query(sql, params) -> [tuple]        a read
    execute(sql, params)                 a write or DDL statement
    insert_many(table, columns, rows, conflict="")   ONE multi-row insert (a real multi-row statement, not a statement per row)
    transaction()                        a context manager: BEGIN .. COMMIT, ROLLBACK on an error
    statements, db_seconds               what the run cost the database: statements sent and wall time spent inside database calls

  SqliteDb      runs here and anywhere.
  PostgresDb    runs against a PostgreSQL server (psycopg2). It is the one piece of this experiment that has NEVER been executed: the sandbox it was written in has no PostgreSQL
                server. It is about 40 lines of standard psycopg2 calls; if Gate P fails on a server, suspect it first.
  ShimPgDb      executes PostgreSQL-flavoured SQL (%s placeholders, `= ANY(%s)` with a list) on SQLite, translating just enough to run, and RECORDS the PostgreSQL text it was given.
                That checks the PostgreSQL path of SqlState (what the dialect emitted, with how many parameters) without a server. It proves nothing about PostgreSQL's behaviour.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys
import time
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.sql_dialect import get_dialect  # noqa: E402


class _Counting:
    def __init__(self):
        self.statements = 0
        self.db_seconds = 0.0
        self.rows_sent = 0

    def _timed(self, fn, rows=0):
        t = time.perf_counter()
        try:
            return fn()
        finally:
            self.db_seconds += time.perf_counter() - t
            self.statements += 1
            self.rows_sent += rows


class SqliteDb(_Counting):
    kind = "sqlite"

    def __init__(self, path: str, synchronous: str = "NORMAL"):
        super().__init__()
        self.dialect = get_dialect("sqlite")
        self.conn = sqlite3.connect(path, isolation_level=None)              # autocommit; transaction() issues BEGIN/COMMIT itself
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute(f"PRAGMA synchronous = {synchronous}")
        self._in_tx = False

    def query(self, sql, params=()):
        return self._timed(lambda: [tuple(r) for r in self.conn.execute(sql, list(params)).fetchall()])

    def execute(self, sql, params=()):
        return self._timed(lambda: self.conn.execute(sql, list(params)).rowcount)

    def executemany(self, sql, rows):
        rows = [list(r) for r in rows]
        if rows:
            self._timed(lambda: self.conn.executemany(sql, rows), rows=len(rows))

    def insert_many(self, table, columns, rows, conflict=""):
        rows = [list(r) for r in rows]
        if not rows:
            return
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)}) {conflict}".strip()
        self._timed(lambda: self.conn.executemany(sql, rows), rows=len(rows))

    @contextmanager
    def transaction(self):
        self.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.execute("ROLLBACK")
            raise
        else:
            self.execute("COMMIT")

    def close(self):
        self.conn.close()


class PostgresDb(_Counting):
    """NEVER RUN against a server (see the module docstring)."""
    kind = "postgres"

    def __init__(self, dsn: str, schema: str = "mdm_gate_p"):
        super().__init__()
        import psycopg2
        import psycopg2.extras
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema):
            raise ValueError(f"schema name {schema!r}: lowercase letters, digits and underscores only")
        self._extras = psycopg2.extras
        self.dialect = get_dialect("postgres")
        self.schema = schema
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True                                         # reads outside a transaction hold nothing; transaction() issues BEGIN/COMMIT itself
        self.cur = self.conn.cursor()
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")           # everything the experiment creates lives in this schema: your own tables are never touched
        self.cur.execute(f"SET search_path TO {schema}")

    def query(self, sql, params=()):
        def run():
            self.cur.execute(sql, list(params) if params else None)
            return [tuple(r) for r in self.cur.fetchall()] if self.cur.description else []
        return self._timed(run)

    def execute(self, sql, params=()):
        def run():
            self.cur.execute(sql, list(params) if params else None)
            return self.cur.rowcount
        return self._timed(run)

    def executemany(self, sql, rows):
        rows = [list(r) for r in rows]
        if rows:
            self._timed(lambda: self._extras.execute_batch(self.cur, sql, rows, page_size=500), rows=len(rows))

    def insert_many(self, table, columns, rows, conflict=""):
        rows = [list(r) for r in rows]
        if not rows:
            return
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s {conflict}".strip()
        self._timed(lambda: self._extras.execute_values(self.cur, sql, rows, page_size=1000), rows=len(rows))

    @contextmanager
    def transaction(self):
        self.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.execute("ROLLBACK")
            raise
        else:
            self.execute("COMMIT")

    def close(self):
        self.cur.close()
        self.conn.close()


class ShimPgDb(SqliteDb):
    """PostgreSQL-flavoured SQL, executed on SQLite; every statement is also kept (in its PostgreSQL form) in `log` for static checks."""
    kind = "postgres-shim"

    def __init__(self, path: str):
        super().__init__(path)
        self.dialect = get_dialect("postgres")
        self.log: list[tuple] = []

    _PLACEHOLDER = re.compile(r"= ANY\(%s\)|%s")

    def _translate(self, sql, params):
        self.log.append((sql, list(params)))
        it, out_params = iter(list(params)), []

        def sub(m):
            value = next(it)
            if m.group(0) != "%s":                                          # `x = ANY(%s)`: a list parameter becomes `x IN (?, ?, ..)`
                if isinstance(value, list):
                    out_params.extend(value)
                    return "IN (" + ", ".join("?" for _ in value) + ")"
                out_params.append(value)
                return "= ANY(?)"
            out_params.append(value)
            return "?"
        return self._PLACEHOLDER.sub(sub, sql), out_params

    def query(self, sql, params=()):
        sql2, p2 = self._translate(sql, params)
        return super().query(sql2, p2)

    def execute(self, sql, params=()):
        if sql in ("BEGIN", "COMMIT", "ROLLBACK"):
            self.log.append((sql, []))
            return super().execute(sql, ())
        sql2, p2 = self._translate(sql, params)
        return super().execute(sql2, p2)

    def executemany(self, sql, rows):
        rows = [list(r) for r in rows]
        if rows:
            self.log.append((sql, rows[0]))
            super().executemany(sql.replace("%s", "?"), rows)

    def insert_many(self, table, columns, rows, conflict=""):
        rows = [list(r) for r in rows]
        if not rows:
            return
        pg_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s {conflict}".strip()       # what PostgresDb sends (execute_values expands the single %s)
        self.log.append((pg_sql, rows[0]))
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)}) {conflict}".strip()
        self._timed(lambda: self.conn.executemany(sql, rows), rows=len(rows))
