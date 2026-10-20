# Database support

## What is supported, and what is not claimed

| Database | Use | Status |
|---|---|---|
| **SQLite** | development, the test suite, small installs | the reference: the whole suite runs on it |
| **PostgreSQL** | production | complete in code and linted (`tests/pglint.py`), **but never run against a live server**: the 14 PostgreSQL variants of the contract suite skip until `TEST_DATABASE_URL` points at a scratch database (see the Postgres runbook) |
| anything else | n/a | **not supported, and not promised** |

## The policy

1. **PostgreSQL is the product's store.** A customer's own databases (Oracle, SQL Server, a warehouse, Salesforce, files) are **sources and targets**, reached through connectors and files, not places the platform stores its data.
   A data warehouse (Snowflake, Databricks, BigQuery) cannot be the store at all: the matching kernel does many small transactional reads and writes, which warehouses are poor at.
2. **Another store is added only for a committed self-hosted need**, in the order the competitors that can be self-hosted use (Ataccama: PostgreSQL, SQL Server and Azure SQL, Oracle "self-managed only";
   Senzing: PostgreSQL, SQL Server and Azure SQL, Oracle 19c or later, and MySQL only below 100M records): **SQL Server / Azure SQL first, then Oracle. Not MySQL**: it would not reach the 100M-record target.
   Each is a `Dialect` subclass, a store of about 1,200 lines that passes the contract suite, the migrations adapted, a tenant-isolation mechanism, and per-engine performance tuning (Senzing publishes a tuning guide for exactly this).
3. **The door stays cheap to open.** That is what the guards below do. Doing it is the point of this file: the guards are in the repository, and they fail loudly.

## What keeps another database cheap (all in the test suite)

| Guard | What it stops | Where |
|---|---|---|
| **One home for SQL text that differs between databases** | `ADD COLUMN`, `RENAME TO`, `CREATE INDEX IF NOT EXISTS`, the JSON type, the catalog queries, the placeholder style, and helpers for new code (`upsert`, `in_list`, `keyset_after`, `versioned_update`, `claim_suffix`) now live in `app/sql_dialect.py`, not scattered through `golden.py` and the migration context | `app/sql_dialect.py` |
| **The SQL actually sent is pinned** | any change to the text sent to SQLite or PostgreSQL by the schema code (8 scenarios, including PostgreSQL, captured BEFORE the move and proven byte-identical after it) | `tests/test_dialect.py`, `tests/data/ddl_snapshot.json` (regenerate with `python3 tests/ddl_snapshot.py`, and say why) |
| **The two stores share one interface** | a method added to only one store; a changed parameter. They had drifted by one method (`find_entities_by_attribute`, PostgreSQL only, used by no application code); it is a named exception | `tests/test_dialect.py` |
| **Tenant isolation does not depend on row-level security** | a read or a WRITE that crosses tenants, tried with the other tenant's REAL ids on every backend: 35 read probes and 10 write probes, with controls proving each probe can see a difference; plus paging when another tenant's ids sort first | `tests/test_store_contract.py` |
| **Nothing new escapes the probes** | a new store method that takes a tenant but is neither probed nor listed (with a reason) in `NOT_YET_PROBED` (20 names today: a visible list of debt) | `tests/test_store_contract.py` |
| **The helpers behave on the real backend** | an upsert, `IN` list, keyset paging over ties, a lost-update version check and a claiming read, executed on SQLite now and on PostgreSQL when a server is available | `tests/test_store_contract.py` |
| **A ratchet on database-specific SQL** | new PostgreSQL-only SQL, raw `execute` calls, driver imports or `if backend == "postgres"` outside the allowed places; and slack (a number that fell must be lowered) | `tests/test_sql_portability.py` |

Checked by deliberate breakage: 11 of 11 breakages of the dialect, the schema code and the interface; **48 of 48** tenant filters removed one at a time from 45 probed store methods; 7 of 7 violations of the ratchet.

## How a backend is added (when the policy says to)

1. Write `class XDialect(Dialect)` in `app/sql_dialect.py`, overriding only what differs (the table in that file's docstring lists the known differences; it is from general knowledge and NOT verified against a server).
2. Write the store (`app/db_x.py`) with the same public methods and parameters as `Store`: `tests/test_dialect.py` fails on any difference.
3. Make it pass `tests/test_store_contract.py` (add a backend class next to `TestSqliteContract` and `TestPostgresContract`: `make_store`, `exec_sql`, `dialect_name`, and the catalog helpers).
4. Adapt the migrations (`app/migrations/versions.py` carries the JSON, partial-index and upsert uses counted in the ratchet) and the migration backend in `runner.py`.
5. Enforce the tenant **in the store's own queries** (the isolation probes will tell you); treat the database's row-level security, if any, as a second layer.
6. Tune it and measure it (the performance gates in `docs/kernel-design.md` apply to it too). Then update the table at the top of this file.

## What is NOT verified or not done

* **PostgreSQL has never run.** The PostgreSQL halves of the new contract tests (`exec_sql`, the helpers, the probes) are unrun code: if one fails on a real server, read it as a bug in that test first.
* **`db_postgres.py` was not rewritten** to use the dialect helpers: its 12 upserts and 14 array lookups are unchanged, because changing that file blind would risk the one thing the pinned SQL guards. They move when a server is available to test the result.
* The 20 methods in `NOT_YET_PROBED` (11 reads that need data the test does not seed, 9 writes that need set-up of their own) are untested for isolation.
* `bulk.py` (now the legacy `--legacy-bulk` engine) runs on local scratch SQLite files whatever the store is; the kernel, which is the default engine, calls only the store methods that already existed, so it needed no new SQL and no dialect work; `setup_rls.py` is PostgreSQL-only by nature.
* `MigrationContext` still branches on how to CALL each driver (cursor against connection): driver plumbing, not SQL text.
