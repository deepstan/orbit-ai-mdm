"""
One object per database for every piece of SQL TEXT that differs between databases.

WHY. Supporting another database (SQL Server, Oracle) must mean writing one Dialect and one store, not hunting for `if backend == "postgres"` through the code. Before this module
the schema code in golden.py and the migration context carried that text inline: `ADD COLUMN`, `RENAME TO`, `CREATE INDEX IF NOT EXISTS`, the JSON column type, the catalog queries, the
placeholder style. They now come from here, and tests/test_dialect.py pins the exact text for the databases we support (tests/data/ddl_snapshot.json was captured BEFORE the move, so
the move provably changed nothing that is sent to a database).

SUPPORTED: SQLite (development, tests, and small installs) and PostgreSQL (production). Nothing else is claimed. Adding one is `class SqlServerDialect(Dialect)` overriding what differs, a store
that passes tests/test_store_contract.py, and tuning; docs/database-support.md has the checklist. How the two other databases the competitors support differ, from general knowledge and NOT
verified against a server (so none of it is implemented):

    construct                 SQLite / PostgreSQL                 SQL Server                             Oracle
    upsert                    INSERT .. ON CONFLICT .. DO UPDATE  MERGE                                  MERGE
    IF NOT EXISTS (DDL)       CREATE INDEX IF NOT EXISTS          IF NOT EXISTS (SELECT ..) CREATE ..    check the catalog first
    add column                ALTER TABLE t ADD COLUMN c T        ALTER TABLE t ADD c T                  ALTER TABLE t ADD (c T)
    rename table              ALTER TABLE a RENAME TO b           EXEC sp_rename 'a', 'b'                ALTER TABLE a RENAME TO b
    row-value comparison      (a, b) < (?, ?)                     not supported: (a < ? OR (a = ? AND b < ?))  as SQL Server
    skip locked rows          FOR UPDATE SKIP LOCKED  (PG only)   WITH (READPAST)                        FOR UPDATE SKIP LOCKED
    IN-list size              SQLite 32766 / PG: one array param  2100 parameters                        1000 values
    JSON column               TEXT / JSONB                        NVARCHAR(MAX)                          CLOB / JSON

The helpers at the bottom (upsert, in_list, keyset_after, claim_suffix, versioned_update) are for NEW code, the matching kernel's tables. The twelve upserts and fourteen array lookups
already in db_postgres.py are NOT rewritten to use them: that file has never run against a server and changing it blind would risk the one thing the pinned SQL guards.
"""
from __future__ import annotations

import json

from .fieldtypes import SQL_TYPES, canonical_type


class Dialect:
    """The standard-SQL defaults (information_schema catalog, `?` placeholders). A database overrides only what differs."""

    name = "standard"
    param = "?"                  # the driver's placeholder; SQL in this code base is written with `?` and translated by sql()
    json_type = "TEXT"
    max_in_values = 500          # how many values one IN (...) list may carry

    # ------------------------------------------------------------------ text translation
    def sql(self, text: str) -> str:
        return text if self.param == "?" else text.replace("?", self.param)

    # ------------------------------------------------------------------ the catalog: does it exist, what is in it
    def table_exists_query(self, name: str) -> tuple:
        return ("SELECT 1 AS present FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?", (name,))

    def column_types_query(self, table: str) -> tuple:
        return ("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ?", (table,))

    def parse_column_types(self, rows: list) -> dict:
        return {r["column_name"]: r["data_type"] for r in rows}

    def columns_query(self, table: str) -> tuple:
        return ("SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ?", (table,))

    def parse_columns(self, rows: list) -> set:
        return {r["column_name"] for r in rows}

    # ------------------------------------------------------------------ DDL. Identifiers arrive already quoted by the caller (golden.q), so the text is exact.
    def create_index_if_not_exists(self, name: str, table: str, columns: str) -> str:
        return f"CREATE INDEX IF NOT EXISTS {name} ON {table}({columns})"

    def add_column(self, table: str, column: str, type_: str) -> str:
        return f"ALTER TABLE {table} ADD COLUMN {column} {type_}"

    def rename_table(self, old: str, new: str) -> str:
        return f"ALTER TABLE {old} RENAME TO {new}"

    def drop_table(self, name: str) -> str:
        return f"DROP TABLE {name}"

    # ------------------------------------------------------------------ types and values
    def column_type(self, field_type: str) -> str:
        return SQL_TYPES[self.name][field_type]

    def canonical_type(self, declared: str) -> str:
        return canonical_type(self.name, declared)

    def json_param(self, value):
        """A JSON value as the driver wants it for a json_type column; empty means NULL."""
        if value is None or value == "":
            return None
        return value if isinstance(value, str) else json.dumps(value)

    # ------------------------------------------------------------------ helpers for NEW code (the kernel's tables)
    def conflict_suffix(self, columns: list, key: list, update: list | None = None) -> str:
        """`ON CONFLICT (key) DO UPDATE SET ..` (default: every non-key column) or, for update=[], `DO NOTHING`. Also what a multi-row insert appends."""
        update = [c for c in columns if c not in key] if update is None else update
        action = "DO NOTHING" if not update else "DO UPDATE SET " + ", ".join(f"{c} = excluded.{c}" for c in update)
        return f"ON CONFLICT ({', '.join(key)}) {action}"

    def upsert(self, table: str, columns: list, key: list, update: list | None = None) -> str:
        """INSERT .. ON CONFLICT (key) DO UPDATE SET (the `update` columns, default: every non-key column); update=[] means DO NOTHING."""
        cols = ", ".join(columns)
        marks = ", ".join("?" for _ in columns)
        return self.sql(f"INSERT INTO {table} ({cols}) VALUES ({marks}) {self.conflict_suffix(columns, key, update)}")

    def row_in(self, columns: list, n_rows: int) -> str:
        """`(a, b) IN (VALUES (?, ?), (?, ?), ..)`: many composite keys in ONE predicate (a DELETE of n key pairs is one statement, not n). SQLite and PostgreSQL; SQL Server and Oracle
        spell this differently."""
        one = "(" + ", ".join("?" for _ in columns) + ")"
        return self.sql(f"({', '.join(columns)}) IN (VALUES {', '.join([one] * n_rows)})")

    def in_list(self, column: str, values) -> tuple:
        """(sql, params) for `column IN values`. An empty list is `1 = 0` (an empty IN () is not valid SQL everywhere). Callers chunk by max_in_values."""
        values = list(values)
        if not values:
            return "1 = 0", []
        if len(values) > self.max_in_values:
            raise ValueError(f"{len(values)} values is more than one IN list may carry on {self.name} ({self.max_in_values}): chunk them with chunks()")
        return self.sql(f"{column} IN ({', '.join('?' for _ in values)})"), values

    def chunks(self, values):
        values = list(values)
        for i in range(0, len(values), self.max_in_values):
            yield values[i:i + self.max_in_values]

    def keyset_after(self, columns: list, descending: bool = False) -> str:
        """A keyset-paging predicate over several columns, as ONE row-value comparison so the planner can seek an index: (a, b) < (?, ?)."""
        return self.sql(f"({', '.join(columns)}) {'<' if descending else '>'} ({', '.join('?' for _ in columns)})")

    def limit_clause(self) -> str:
        return self.sql("LIMIT ?")

    def claim_suffix(self) -> str:
        """Appended to a SELECT that CLAIMS rows for one worker, so a second worker skips them instead of waiting. Empty where the database has a single writer."""
        return ""

    def versioned_update(self, table: str, set_columns: list, key: list, version: str = "version") -> str:
        """An optimistic-concurrency update. Parameters, in order: the new values, the key values, the version the caller READ. The caller checks that exactly one row changed."""
        sets = ", ".join([f"{c} = ?" for c in set_columns] + [f"{version} = {version} + 1"])
        where = " AND ".join([f"{c} = ?" for c in key] + [f"{version} = ?"])
        return self.sql(f"UPDATE {table} SET {sets} WHERE {where}")


class SqliteDialect(Dialect):
    name = "sqlite"
    max_in_values = 900          # older SQLite builds allow 999 variables

    def table_exists_query(self, name: str) -> tuple:
        return ("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (name,))

    def column_types_query(self, table: str) -> tuple:
        return (f"PRAGMA table_info({table})", ())

    def parse_column_types(self, rows: list) -> dict:
        return {r["name"]: (r["type"] or "").upper() for r in rows}

    def columns_query(self, table: str) -> tuple:
        return (f"PRAGMA table_info({table})", ())

    def parse_columns(self, rows: list) -> set:
        return {r["name"] for r in rows}


class PostgresDialect(Dialect):
    name = "postgres"
    param = "%s"
    json_type = "JSONB"
    max_in_values = 10_000       # one array parameter in practice; the cap only keeps a statement's work bounded

    def json_param(self, value):
        if value is None or value == "":
            return None
        from psycopg2.extras import Json
        return Json(json.loads(value) if isinstance(value, str) else value)

    def in_list(self, column: str, values) -> tuple:
        values = list(values)
        if not values:
            return "1 = 0", []
        return f"{column} = ANY(%s)", [values]

    def claim_suffix(self) -> str:
        return " FOR UPDATE SKIP LOCKED"


_DIALECTS = {"sqlite": SqliteDialect(), "postgres": PostgresDialect()}


def get_dialect(name: str) -> Dialect:
    try:
        return _DIALECTS[name]
    except KeyError:
        raise ValueError(f"No SQL dialect for '{name}'. Supported: {', '.join(sorted(_DIALECTS))}. docs/database-support.md says how one is added.") from None
