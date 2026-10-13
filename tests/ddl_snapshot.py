"""
Records, for each backend, the EXACT SQL the golden-table and migration-context code sends to the database, by driving it through fake connections. Nothing here needs a
database server, so the PostgreSQL text is checked even though no Postgres is available; tests/test_dialect.py compares it with tests/data/ddl_snapshot.json, which was captured
BEFORE the dialect code was gathered into app/sql_dialect.py. A change in this output is a change in what is sent to a customer's database: it must be deliberate.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import golden  # noqa: E402
from app.migrations.runner import MigrationContext  # noqa: E402


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Script:
    """What the fake database 'contains': whether the table exists, its columns, and its rows."""

    def __init__(self, backend, exists=True, columns=None, rows=None):
        self.backend, self.exists, self.columns, self.rows = backend, exists, columns or {}, rows or []

    def answer(self, sql):
        s = " ".join(sql.split())
        if "sqlite_master" in s:
            return [{"name": "entity_customer"}] if self.exists else []
        if "information_schema.tables" in s:
            return [{"present": 1}] if self.exists else []
        if s.startswith("PRAGMA table_info"):
            return [{"name": c, "type": t} for c, t in self.columns.items()]
        if "information_schema.columns" in s:
            return [{"column_name": c, "data_type": t} for c, t in self.columns.items()]
        if s.startswith("SELECT COUNT(*) AS n FROM"):
            return [{"n": len(self.rows)}]
        if s.startswith("SELECT * FROM"):
            return list(self.rows)
        return []


class FakeSqlite:
    def __init__(self, script, log):
        self.script, self.log = script, log

    def execute(self, sql, params=()):
        self.log.append(["execute", sql, [None if p is None else str(p) for p in params]])
        return _Result(self.script.answer(sql))

    def executemany(self, sql, rows):
        self.log.append(["executemany", sql, [[None if v is None else str(v) for v in r] for r in rows]])


class FakePgCursor:
    def __init__(self, script, log):
        self.script, self.log, self._rows = script, log, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append(["execute", sql, None if params is None else [None if p is None else str(p) for p in params]])
        self._rows = self.script.answer(sql)

    def executemany(self, sql, rows):
        self.log.append(["executemany", sql, [[None if v is None else str(v) for v in r] for r in rows]])

    def fetchall(self):
        return self._rows


class FakePg:
    def __init__(self, script, log):
        self.script, self.log = script, log

    def cursor(self):
        return FakePgCursor(self.script, self.log)


def context(backend, **script):
    log = []
    s = _Script(backend, **script)
    conn = FakeSqlite(s, log) if backend == "sqlite" else FakePg(s, log)
    return MigrationContext(backend, conn), log


FIELDS = [("first_name", "text"), ("visits", "integer"), ("balance", "numeric"), ("joined", "timestamp")]
SYSTEM = ["entity_id", "tenant_id", "status", "created_at", "updated_at", "quality_issues"]


def _have(backend, types):
    return types[backend]


def record() -> dict:
    out = {}
    for backend in ("sqlite", "postgres"):
        text, big, num, ts = {"sqlite": ("TEXT", "INTEGER", "NUMERIC", "TIMESTAMP"), "postgres": ("text", "bigint", "numeric", "timestamp with time zone")}[backend]
        sysc = {c: text for c in SYSTEM}
        scenarios = {}
        ctx, log = context(backend, exists=False)
        golden.ensure_table(ctx, "customer", FIELDS)
        scenarios["new_table"] = log
        ctx, log = context(backend, exists=True, columns={**sysc, "first_name": text})            # a table that lacks three columns and quality_issues
        del ctx
        have = {c: t for c, t in sysc.items() if c != "quality_issues"}
        have["first_name"] = text
        ctx, log = context(backend, exists=True, columns=have)
        golden.ensure_table(ctx, "customer", FIELDS)
        scenarios["add_columns"] = log
        wrong = {**sysc, "first_name": text, "visits": big, "balance": text, "joined": ts, "old_field": text}   # balance is TEXT but the config says numeric: a retype
        rows = [{"entity_id": f"e{i}", "tenant_id": "t", "status": "active", "created_at": "2026-01-01", "updated_at": "2026-01-02", "quality_issues": None,
                 "first_name": f"n{i}", "visits": str(i), "balance": f"{i}.50", "joined": "2026-01-01T00:00:00", "old_field": "keep"} for i in range(2)]
        ctx, log = context(backend, exists=True, columns=wrong, rows=rows)
        golden.ensure_table(ctx, "customer", FIELDS)
        scenarios["retype"] = log
        ctx, log = context(backend, exists=True, columns=sysc)
        ctx.table_exists("entity_customer"); ctx.column_types("entity_customer"); ctx.columns("entity_customer")
        ctx.execute("INSERT INTO t (a, b) VALUES (?, ?)", ("x", "y")); ctx.execute("CREATE TABLE z (a TEXT)"); ctx.executemany("INSERT INTO t VALUES (?)", [("p",), ("q",)])
        ctx.query("SELECT a FROM t WHERE b = ?", ("v",))
        scenarios["context_basics"] = log
        out[backend] = scenarios
    return out


if __name__ == "__main__":
    import json
    path = os.path.join(os.path.dirname(__file__), "data", "ddl_snapshot.json")
    with open(path, "w") as f:
        json.dump(record(), f, indent=1, sort_keys=True)
    print("wrote", path)
