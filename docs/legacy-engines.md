# Retiring the old engines (a plan; nothing has been deleted)

Status (2026-10-19): the kernel is the application's engine. The old engines are kept for two reasons only: **rollback** (`MDM_ENGINE=legacy`) and **comparison**. Using either now logs a one-time DEPRECATED notice
(`MDM_ENGINE=legacy`, `--legacy-bulk`, `ENABLE_LLM_ADJUDICATION`). Nothing is removed.

## What is old (found by searching the code for the word "legacy" and for the set-based engine's module; not an audit)

* the pairwise decision step: `_route_record_legacy` and its helpers in `app/pipeline.py`, selected by `kernel.engine()` in `app/kernel.py`
* the set-based bulk engine: `app/bulk.py`, reached by `bulk_ingest.py --legacy-bulk` (or `--bulk` while `MDM_ENGINE=legacy`)
* tests that pin the old behaviour (`tests/legacy_engine.py`, and the classes decorated with it) and the comparison commands in `benchmark.py` (`equivalence`, `engines`)
* `app/llm_adjudication.py`, called from one place in `app/kernel_router.py`; superseded by the agent runner (`docs/agent-runner.md`)

## When to remove them (all of these)

1. The kernel has run on **PostgreSQL**: runbook steps 6 to 8 pass on your server (they have never been run there), including a load of at least the size you care about.
2. **Gate P** has given the per-worker number, so we know the kernel can meet the volume (the set-based engine was ~12x faster on a first load: removing it must not leave a gap).
3. A **disagreement report** (where the kernel and the old engine differ on your data) has been read by stewards, so that what is lost is known (the kernel keeps a record out of a wrong group; the old engine chains).
4. One release has carried the deprecation notices, and nobody needed the rollback.
5. For the old adjudication specifically: the agent runner has produced **shadow-mode results** (agreement, false merges) on your data, so there is something to compare with.

## What removal would do, so it can be reviewed first

Delete the files and functions above; remove `MDM_ENGINE` (or keep it accepting only `kernel`); remove `--legacy-bulk`; delete the legacy-pinned tests and `tests/legacy_engine.py`; the ratchet baselines in
`tests/test_sql_portability.py` come down with the code. Data made by either engine stays valid (a record's group is the same table either way). Re-run the full suite and the runbook's steps 6 to 8 afterwards.

## Until then

Do not extend the old engines. A fix that only they need is a reason to move the user to the kernel, not to patch them.
