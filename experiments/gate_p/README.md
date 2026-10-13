# Gate P: how fast can ONE worker resolve records when the kernel's state lives in your PostgreSQL?

This is the number that decides whether the 100-million-record plan is reachable. It cannot be measured where this was written (no PostgreSQL server), so it is meant to be run on YOURS.

## What you need
* Python 3.10+ and `pip install psycopg2-binary`
* A scratch PostgreSQL database and a user that can `CREATE SCHEMA` and `CREATE TABLE` in it. Everything the experiment creates lives in the schema `mdm_gate_p` (change it with `--schema`); your own tables are never touched.
* Run it from a machine as near the database as the real load will be (same region, same network). **Where you run it is part of the answer**: the round trip time to the database is printed first, and it often explains everything else.
* From the project root. Use `GATE_P_DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME` or `--db`.

## The runs, in order (each prints a table and writes a JSON report; please send the JSON files back)
1. **Is it connected?** `python3 experiments/gate_p/gate_p.py --check`  (creates the tables, writes and reads a few rows, times a trivial round trip)
2. **The baseline.** `python3 experiments/gate_p/gate_p.py --records 100000 --out gate_p_100k.json`  (naive is capped at 10,000 arrivals because it is slow)
3. **The one that matters: the cache SMALLER than the data.** `python3 experiments/gate_p/gate_p.py --records 200000 --modes hot --cache-groups 20000 --cache-buckets 60000 --cache-records 60000 --out gate_p_small_cache.json`
   At 100 million records nothing fits in memory. On SQLite this made the rate FALL all the way through the run (834 to 457 records/s). Look at the "rate by window" line.
4. **Does the database scale with workers?** `python3 experiments/gate_p/gate_p.py --records 50000 --modes hot --workers 4 --out gate_p_workers.json` (each worker has its own tenant and data in the same database: independent partitions, not yet concurrent writers on one group).
5. Optional, small and slow: `--records 5000 --verify` checks that the database-backed kernel gives exactly the in-memory kernel's groups and events on YOUR server (it does on SQLite).

## How to read a report
* `records_per_second` and `windows`: the rate over the run. A rate that keeps falling means the tables or the cache misses are growing faster than the database copes. **Use the last window, not the average.**
* `statements_per_record`, `mean_statement_ms`, `database_share`: if the database share is high, batching and fewer statements are the lever; if it is low, the time is in Python (scoring), and ranking and caching are the lever.
* `arrival_latency_ms` p99 and `flush_ms`: the pause when a batch is flushed (a delta feed cares about it; a bulk load does not).
* `round_trip`: if p50 is a millisecond or more, a statement-per-record design could never work; that is the argument for what the batching does.
* `projection`: the workers needed for `--target-rate` (default 2,315 = 100M in 12 hours) at the rate measured. It assumes linear scaling: run 4 to see.

## What has and has not been verified
* VERIFIED on SQLite (tests, `experiments/gate_p/test_gate_p.py`, 21 tests, 12 of 12 deliberate breakages caught): the groups, the whole event stream, the links and the reviews are identical to the in-memory kernel's in every mode and with tiny caches; unflushed writes are visible to reads; a flush is one transaction and a failure leaves nothing half written; the PostgreSQL TEXT the code emits has the right placeholders (run on SQLite through a shim).
* **NOT VERIFIED: `PostgresDb` in `dbs.py`**, about 40 lines of psycopg2 calls, has never touched a server; neither has `--workers` (the sandbox had one core) or `--drop`. If a run fails, the traceback is the most valuable thing you can send.
* PostgreSQL's behaviour (plans, locking, the cost of the multi-row statements, `execute_values`) is exactly what is being measured; nothing here predicts it.
* Concurrent writers on the SAME group are not handled (one writer per tenant): that is Gate S, after this.
