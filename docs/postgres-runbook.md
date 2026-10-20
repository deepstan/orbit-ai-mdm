> **A SNAPSHOT of the shared runbook document, taken 2026-10-14 (revision 131), so the runbook travels with the code.**
> **The shared document is the master:** https://claude.ai/code/artifact/6483a067-7048-4404-884c-f1d850148ee7 . If the two differ, trust the shared document and update this copy.
> (Exported to Markdown, so a few underscores in table cells are escaped; the commands were checked against the code: every script and flag they use exists.)

# Running the MDM platform on PostgreSQL: fresh-install runbook

Oct 8, 2026 · @John

Updated 2026-10-16 (migration 017, agent recommendations, step 12e; step 10 corrected) and 2026-10-14: the matching engine is now the kernel, one engine for every way a record arrives (steps 6 to 8 and 9d have new expected outputs and numbers, and 7d now checks the set-based engine's refusal); test counts are 1,191 (1,146 pass, 45 skip); the matching engine writes in batches (see 8a and 8b); the engine settings are in the reference tables. Step 11 (Gate P) and the cache settings were added earlier the same day.

## Before you start

The Postgres backend has never run against a live server; this runbook is how you find out whether it works, one gate at a time.

Every test I could run (1,146 pass) ran on SQLite. The Postgres store implements every method the engines call, but none of its SQL has executed on a live server: onboarding and delta loads, unmerge, the audit counts and queue, the paged review queue and entity list, and migrations 014 to 018 included. Forty-five Postgres tests skip without a server: 17 in `test_postgres_backend.py` and 28 in `test_store_contract.py`. That second file runs the same scenarios on SQLite here, so a failure on Postgres points at the SQL, not at the test. One exception: the small helper that runs the dialect checks on Postgres (exec\_sql) is new and has never run, so read a failure in test\_the\_dialect\_helpers\_behave\_on\_this\_backend as a possible bug in that helper first. The matching engine is now the kernel; it adds no SQL of its own (it calls the store methods above), but its pattern of calls has never run on Postgres either, and steps 6 to 8 are what test it.

How to use it:

- Work through steps 1 to 7 in order. Each ends with a pass condition and the numbers to expect.
- Stop at the first step that fails and send me the output (see "When something fails").
- Steps 8 to 15 are optional: query plans, row-level security, the API, Gate P, the read-only MCP server for AI agents, replaying decisions through an agent, running our own agent in shadow mode, and trying the key filter.

One safety rule: use a scratch database you can drop. The sample loads write real rows, and nothing here needs production data.

## What you need

A PostgreSQL server you can create a database on, Python with a virtual environment, and the `psycopg2-binary` driver. The code uses `psycopg2`, not `psycopg` (version 3).

| Item | What to use | Note |
| --- | --- | --- |
| PostgreSQL | Your PostgreSQL 18 | The code needs JSONB, `ON CONFLICT`, partial indexes and row-level security, all present since 9.5. I have tested no version. |
| Python | 3.12 or 3.13 if you can | Developed and tested on 3.12. You have 3.14: I do not know whether `psycopg2-binary` has a prebuilt wheel for it. If `pip` fails, make the virtual environment with 3.12 or 3.13. |
| Driver | `psycopg2-binary` | Not in `requirements.txt` by default; step 2 installs it. |
| Role | A role that can create databases | The `postgres` superuser is fine for the first run. Step 9 uses a restricted role. |
| Disk | A few hundred MB free | Only the set-based engine (--legacy-bulk) keeps temporary work files on local disk (always SQLite files), whatever the target database; the default engine needs none. |
| The code | `customer-mdm-mvp.zip` | It contains `samples/` with the CSV files used below. |

Commands are given for PowerShell, which you use, with bash alongside. On Windows use `python`; the README says `python3`.

## Step 1: Create a scratch database

Create two empty databases, one for the runbook loads and one for the Postgres test suite, and confirm you can connect. `psql` must be on your PATH; on Windows that is the `bin` folder of your PostgreSQL install.

```powershell
psql -U postgres -h localhost -c "CREATE DATABASE mdm_runbook;"
psql -U postgres -h localhost -c "CREATE DATABASE mdm_test;"
psql -U postgres -h localhost -d mdm_runbook -c "SELECT version();"
```

Pass: the last command prints your PostgreSQL version. The same three commands work in bash.

## Step 2: Install the code

Unzip the project, make a virtual environment, and install the requirements plus the Postgres driver.

```powershell
Expand-Archive customer-mdm-mvp.zip -DestinationPath .
cd customer-mdm-mvp
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install psycopg2-binary pyyaml
python -c "import psycopg2; print(psycopg2.__version__)"
```

bash equivalent:

```bash
unzip customer-mdm-mvp.zip && cd customer-mdm-mvp
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt psycopg2-binary pyyaml
```

Pass: the last PowerShell command prints a version number. If PowerShell refuses to run the activation script, run `Set-ExecutionPolicy -Scope Process RemoteSigned` and activate again. `pyyaml` is needed in step 8: the domain templates folder contains a YAML file, and loading it fails without PyYAML.

If `pip install psycopg2-binary` fails on Python 3.14, create the environment with 3.12 or 3.13 instead.

## Step 3: Set the environment

Point the scripts at the scratch database with `DATABASE_URL`, and the Postgres tests at the second one with `TEST_DATABASE_URL`. Replace `YOUR_PASSWORD`; if it contains characters such as `@` or `/`, URL-encode them.

```powershell
$env:DATABASE_URL = "postgresql://postgres:YOUR_PASSWORD@localhost:5432/mdm_runbook"
$env:TEST_DATABASE_URL = "postgresql://postgres:YOUR_PASSWORD@localhost:5432/mdm_test"
```

```bash
export DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/mdm_runbook
export TEST_DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/mdm_test
```

With `DATABASE_URL` set, `migrate.py`, `bulk_ingest.py`, `domains_cli.py` and the API all use Postgres; without it they use the SQLite file `mdm.db`.

Run the main unit test suite (1,191 tests, about four minutes) in a shell where `DATABASE_URL` is **not** set. I ran it that way, and I have not checked whether any test reads that variable.

`MDM_TOKEN_KEY` is needed only for the person domain, whose SSN is hashed. If you try it, set it to any string of at least 32 characters. The customer domain used below does not need it.

The matching engine is the kernel by default. `MDM_ENGINE=legacy` selects the older pairwise engine; leave it unset for this runbook (the engine settings are in the environment table at the end).

## Step 4: Apply the schema

The database has 18 migrations to apply. Look at what is pending, read the SQL that would run, apply it, and check the result.

```powershell
python migrate.py status
python migrate.py upgrade --dry-run
python migrate.py upgrade
python migrate.py status
```

Pass: the first `status` lists migrations as pending; after `upgrade`, `status` lists them all as applied and exits with code 0 (it exits 1 while anything is pending).

Then check from `psql` that the pieces Postgres needs are really there:

```powershell
psql $env:DATABASE_URL -c "\dt"
psql $env:DATABASE_URL -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'standardized_records' AND column_name IN ('matching', 'cleansed');"
psql $env:DATABASE_URL -c "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_source_records_unfinished';"
```

Expect: the tables listed (including `source_records`, `standardized_records`, `blocking_keys`, `match_candidates`); both columns of type `jsonb`; and a partial index whose definition ends with `WHERE (processing_status <> 'matched'::text)`. These are my expectations from reading the migrations; if what you see differs, send it to me.

The per-domain golden tables, such as `entity_customer`, are created the first time data is loaded.

Migration 017 adds the table `agent_recommendations`. Check it: `psql $env:DATABASE_URL -c "\d agent_recommendations"` should list the columns (with `confidence` as `double precision`), the unique index `idx_agent_recs_unique`, the index `idx_agent_recs_recent`, and two check constraints (the recommendation must be confirm, reject or needs_human; the confidence must be between 0 and 1). Migration 018 adds one column to it, `outcome_seen` (a small integer, empty until a person decides an item that had a suggestion).

Also check the indexes that migrations 015 and 016 add; the audit page and the paged lists depend on them:

```powershell
psql $env:DATABASE_URL -c "SELECT indexname, indexdef FROM pg_indexes WHERE indexname IN ('idx_match_candidates_pending', 'idx_merge_audits_queue', 'idx_merge_audits_scope') ORDER BY 1;"
psql $env:DATABASE_URL -c "SELECT indexname FROM pg_indexes WHERE indexname LIKE 'idx_entity_%_tenant_entity' ORDER BY 1;"
```

Expect two rows from the first: `idx_match_candidates_pending`, a partial index on `(tenant_id, domain, score DESC, id DESC)` whose definition ends with `WHERE ((status)::text = 'pending'::text)`, and `idx_merge_audits_queue` on `(tenant_id, domain, verdict, made_by, sampled_at)`. `idx_merge_audits_scope` must be absent (015 replaced it). The second query lists one `..._tenant_entity` index for each golden table that already exists; after step 6 there will be one for `entity_customer` too.

## Step 5: Run the Postgres tests

The 17 tests in `tests/test_postgres_backend.py` are the first real SQL this backend has run. They need `TEST_DATABASE_URL` and `psycopg2`; without both they skip silently, so check the count.

```powershell
python -m unittest tests.test_postgres_backend -v
```

Pass: `Ran 17 tests`, `OK`, and no `skipped`. They cover ingest and retrieval, auto-merge, tenant isolation, lineage, API keys, the audit chain, the blocking index, JSONB round trips, row-level-security policies, migrations, reindex, typed golden columns, processing state, and purge.

Then run the contract suite. It is one set of scenarios for everything added after this runbook was first written (unmerge and separations, the audit counts and queue, the pages of the review queue and the entities, migrations 014 to 016 and their indexes, tenant isolation on every read and every write, and the dialect helpers: upsert, IN list, keyset paging over ties, a version check, and the unit of work: batch and savepoint), run on SQLite and on your Postgres:

```powershell
python -m unittest tests.test_store_contract -v
```

Pass: `Ran 55 tests`, `OK`, no `skipped` (27 scenarios on SQLite, the same 27 on Postgres, and one Postgres-only check that the queue and entity queries can use their indexes). If a Postgres test fails while its SQLite twin passes, it is a bug in `db_postgres.py` or in migrations 014 to 016: send me the traceback. These tests use a tenant of their own each but leave rows in `mdm_test`.

A static check of every Postgres statement, for placeholder and percent-escaping mistakes, needs no server (`python -m unittest tests.test_postgres_sql_lint`). It found nothing in the 148 statements, which does not mean they are valid Postgres: only a Postgres can say that.

Then run the broader checker, which prints PASS, FAIL or SKIP per section (environment, unit tests, API over HTTP, access control, Postgres, and two optional ones). Unset `DATABASE_URL` for this run, so its unit-test section stays on SQLite, and keep `TEST_DATABASE_URL`:

```powershell
Remove-Item Env:DATABASE_URL
python validate_setup.py
```

Its text still says "48 tests"; the suite is now far larger, so ignore that number. Set `DATABASE_URL` again before step 6.

If a test fails, the traceback is what I need. A failing SQL statement here is exactly what this runbook exists to find.

## Step 6: Load the sample data two ways

Load the same 200-record file twice, once with `--bulk` and once without, each into its own tenant. Both commands run the same engine (the kernel) through the same code path, so they must give identical results: 165 entities, 35 automatic merges and 1 pair for review. Those are the numbers I got on SQLite, and the result must not depend on the database.

```powershell
$MAP = "first_name=First Name,last_name=Last Name,email=Email,phone=Phone,address_line1=Address,city=City,state=State,postal_code=Zip"

python bulk_ingest.py --tenant acme_bulk --domain customer --file samples/sample_customers_200.csv --mapping "$MAP" --id-column CustomerID --source-system crm --bulk

python bulk_ingest.py --tenant acme_regular --domain customer --file samples/sample_customers_200.csv --mapping "$MAP" --id-column CustomerID --source-system crm
```

In bash, set `MAP='first_name=First Name,...'` the same way and use `--mapping "$MAP"`.

Each command should end with `Done: 200 records in ... s`, then `Auto-merged: 35`, `Queued for review: 1` and `Standalone (new): 164`, and the line `1 record(s) need steward review`.

Check both tenants in the database:

```powershell
psql $env:DATABASE_URL -c "SELECT tenant_id, COUNT(*) AS records FROM source_records WHERE tenant_id IN ('acme_bulk','acme_regular') GROUP BY tenant_id ORDER BY 1;"
psql $env:DATABASE_URL -c "SELECT tenant_id, COUNT(*) AS active_entities FROM entity_customer WHERE status = 'active' AND tenant_id IN ('acme_bulk','acme_regular') GROUP BY tenant_id ORDER BY 1;"
psql $env:DATABASE_URL -c "SELECT tenant_id, status, COUNT(*) FROM match_candidates WHERE tenant_id IN ('acme_bulk','acme_regular') GROUP BY tenant_id, status ORDER BY 1, 2;"
```

| Tenant | Records | Active entities | Candidates |
| --- | --- | --- | --- |
| `acme_bulk` | 200 | 165 | confirmed 35, pending 1 |
| `acme_regular` | 200 | 165 | confirmed 35, pending 1 |

Pass: both rows match the table. If the two tenants differ from each other, or from the table, send me both sets of query results.

## Step 7: Onboarding and delta loads

These paths modify existing rows (a record joins a group, a changed record leaves one or moves to another, two groups merge), so they are the most likely to expose a Postgres problem. Onboarding and delta are not separate modes in the default engine: a normal load classifies each record as new, unchanged or changed, and `--onboard` and `--delta` are accepted but not needed. All numbers below are from the same sequence run on SQLite.

### 7a: Onboard a second source onto existing data

Split the file in two, load the first 100 records, then load the other 100 into the same tenant.

```powershell
python -c "import csv; r = list(csv.reader(open('samples/sample_customers_200.csv', newline=''))); csv.writer(open('part1.csv', 'w', newline='')).writerows(r[:101]); csv.writer(open('part2.csv', 'w', newline='')).writerows([r[0]] + r[101:])"

python bulk_ingest.py --tenant acme_onboard --domain customer --file part1.csv --mapping "$MAP" --id-column CustomerID --source-system crm --bulk

python bulk_ingest.py --tenant acme_onboard --domain customer --file part2.csv --mapping "$MAP" --id-column CustomerID --source-system crm --bulk --onboard
```

Expect, for the first load, `Auto-merged: 11`, `Queued for review: 0` and `Standalone (new): 89` (so 89 entities); for the second, `Auto-merged: 24`, `Queued for review: 1` and `Standalone (new): 75`. Pass: tenant `acme_onboard` ends with 200 records, 165 active entities, and candidates confirmed 35, pending 1: exactly step 6's result, reached in two loads.

### 7b: Send the whole file again as a delta

Nothing has changed, so nothing should be written.

```powershell
python bulk_ingest.py --tenant acme_onboard --domain customer --file samples/sample_customers_200.csv --mapping "$MAP" --id-column CustomerID --source-system crm --bulk --delta
```

Pass: `Already held, unchanged: 200   |   changed, new version: 0`, no merges or reviews reported, and the 7a counts are unchanged.

### 7c: Apply a delta that changes every record

`sample_customers_200_v2.csv` has the same 200 customer ids with different content. Apply it to the tenant from step 6.

```powershell
python bulk_ingest.py --tenant acme_bulk --domain customer --file samples/sample_customers_200_v2.csv --mapping "$MAP" --id-column CustomerID --source-system crm --bulk --delta

psql $env:DATABASE_URL -c "SELECT status, COUNT(*) FROM entity_customer WHERE tenant_id = 'acme_bulk' GROUP BY status ORDER BY 1;"
psql $env:DATABASE_URL -c "SELECT status, COUNT(*) FROM match_candidates WHERE tenant_id = 'acme_bulk' GROUP BY status ORDER BY 1;"
psql $env:DATABASE_URL -c "SELECT (SELECT COUNT(*) FROM source_record_history WHERE tenant_id = 'acme_bulk') AS history_rows, (SELECT COUNT(*) FROM source_records WHERE tenant_id = 'acme_bulk' AND version = 2) AS at_version_2;"
```

Expect `Already held, unchanged: 0   |   changed, new version: 200`. A changed record is matched again like a new one, so it can leave its group or move to another; that is why there are superseded pairs and merged entities below. Then:

| Check | Expected |
| --- | --- |
| Entities by status | active 165, merged 26 |
| Candidates by status | confirmed 35, pending 3, superseded 42 |
| History rows | 200 |
| Records at version 2 | 200 |

Stronger check: load `samples/sample_customers_200_v2.csv` fresh into a new tenant (`acme_v2`, the same command as step 6 with that file). Updating in place must give the same groups as loading fresh: 165 active entities and 35 confirmed pairs in both. On SQLite I compared the groups themselves, record by record, and they were identical.

### 7d: Check the set-based engine's refusal

The default engine accepts a load into a tenant that already has data (a load is always safe to repeat). The set-based engine, selected with `--legacy-bulk`, builds a domain from nothing and must refuse a tenant that already has data, not silently merge.

```powershell
python bulk_ingest.py --tenant acme_onboard --domain customer --file part2.csv --mapping "$MAP" --id-column CustomerID --source-system crm --legacy-bulk
$LASTEXITCODE
```

Pass: a note that `--legacy-bulk` uses the set-based pair engine, then an `Error: tenant 'acme_onboard' already has 'customer' records...` message that mentions `--onboard`, and exit code 2. Also, `--bulk --workers 4` must be refused with exit code 2 and a message that names `--legacy-bulk`: those options belong to the set-based engine.

## Step 8 (optional): People at scale and the query plans

This step checks that Postgres answers the engine's main lookups with index lookups, which only shows once the tables are large. Use the person domain: its blocking keys scale, while the customer domain's keys (every 4-digit window of the phone number) make any file of tens of thousands of customers dense and slow.

Set two variables for this step only. `MDM_DOMAINS_DIR` registers the person domain; `MDM_TOKEN_KEY` hashes its SSN.

```powershell
$env:MDM_DOMAINS_DIR = "domain_templates"
$env:MDM_TOKEN_KEY = "a-long-secret-of-at-least-32-characters-0123456789"
$PMAP = "first_name=first_name,last_name=last_name,dob=dob,ssn=ssn,phone=phone,email=email,address_line1=address_line1,postal_code=postal_code"
```

### 8a: A small reference load

```powershell
python samples/make_people_csv.py 4000 > people_4000.csv
python bulk_ingest.py --tenant people --domain person --file people_4000.csv --mapping "$PMAP" --id-column PersonID --source-system crm --bulk
```

Expect `Done: 4000 records`, `Auto-merged: 256`, `Queued for review: 59` and `Standalone (new): 3685`. In the database, tenant `people` should have 4,000 records, 3,744 active rows in `entity_person`, and candidates confirmed 256, pending 61. (The set-based engine, `--legacy-bulk`, gives 3,743 entities, 268 and 65; it is a different engine, and the two agree on 267 of its 269 same-entity pairs.) On SQLite the load takes about 5 seconds (about 750 records per second).

### 8b: Load 50,000 people and time it

```powershell
python samples/make_people_csv.py 50000 > people_50000.csv
$t = Get-Date
python bulk_ingest.py --tenant people50k --domain person --file people_50000.csv --mapping "$PMAP" --id-column PersonID --source-system crm --bulk
(Get-Date) - $t
```

Expect `Done: 50000 records` and, in the database, 50,000 matched records, 46,650 active entities, and candidates confirmed 3,350, pending 745 (the set-based engine, `--legacy-bulk`, gives 46,634). On SQLite on one core the default engine took about 101 s (about 490 records per second on average; it slows as the data grows, from about 650 per second over the first 10,000 to about 420 over the last 25,000); the set-based engine took 51 s. Before the writes were batched it took about 10 minutes. On PostgreSQL every statement is a network round trip, so expect it to be slower than on SQLite: that is what this step measures. Send me your time: it is the first real Postgres timing.

### 8c: Look at the plans

```powershell
psql $env:DATABASE_URL -c "ANALYZE;"
psql $env:DATABASE_URL -c "SELECT id FROM source_records WHERE tenant_id = 'people50k' LIMIT 3;"
psql $env:DATABASE_URL -c "SELECT entity_id FROM entity_person WHERE tenant_id = 'people50k' LIMIT 3;"
psql $env:DATABASE_URL -c "SELECT blocking_key FROM blocking_keys WHERE tenant_id = 'people50k' LIMIT 3;"
```

Then, in `psql`, run these with the values just printed in place of the placeholders:

```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT id, entity_id FROM source_records WHERE tenant_id = 'people50k' AND domain = 'person' AND id = ANY(ARRAY['<id1>','<id2>','<id3>']);
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM entity_person WHERE tenant_id = 'people50k' AND entity_id = ANY(ARRAY['<e1>','<e2>','<e3>']);
EXPLAIN (ANALYZE, BUFFERS) SELECT blocking_key, COUNT(*) FROM blocking_keys WHERE tenant_id = 'people50k' AND domain = 'person' AND blocking_key = ANY(ARRAY['<k1>','<k2>','<k3>']) GROUP BY blocking_key;
```

Pass: each plan uses an index (`Index Scan`, `Index Only Scan` or `Bitmap Index Scan`) and takes about a millisecond. A `Seq Scan` on these 50,000-row tables means the full-scan problem I fixed in SQLite also exists on Postgres; send me that plan.

### 8d: The two paged lists

The review queue and the entity list read a page through an index, so a page should cost the same however far down it is. With only a few hundred pending pairs Postgres may reasonably choose a sequential scan, so turn that off first, to ask whether the index can serve the query at all:

```sql
SET enable_seqscan = off;
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM match_candidates WHERE tenant_id = 'people50k' AND domain = 'person' AND status = 'pending' AND (score, id) < (0.8, 'cand_zzzz') ORDER BY score DESC, id DESC LIMIT 51;
EXPLAIN (ANALYZE, BUFFERS) SELECT entity_id FROM entity_person WHERE tenant_id = 'people50k' AND entity_id > 'ent_5' AND status = 'active' ORDER BY entity_id LIMIT 101;
```

Pass: the first plan is an `Index Scan using idx_match_candidates_pending` whose `Index Cond` contains the `(score, id)` comparison, with no `Sort` node; the second uses `idx_entity_person_tenant_entity` with no `Sort`; each takes about a millisecond. A `Sort` node, or a plan that reads far more rows than it returns, means the cursor does not seek on Postgres: send me the plan.

When you are done, unset the two variables so later steps use the built-in customer domain:

```powershell
Remove-Item Env:MDM_DOMAINS_DIR, Env:MDM_TOKEN_KEY
```

## Step 9 (optional): Row-level security and a least-privilege role

This is the least-tested part of the project: its own script says it has never run against a real Postgres server. Do it after steps 6 and 7, because the role is granted access to the tables that exist when you create it, golden tables included.

### 9a: Create the role and apply the policies

Run both as the owner (your `postgres` connection):

```powershell
python setup_rls.py create-role --role-name mdm_app --password CHANGE_ME
python setup_rls.py enable
```

If `create-role` fails with `relation ... does not exist`, a golden table is missing; run `python domains_cli.py reindex customer` and try again.

### 9b: Connect as the restricted role and verify

```powershell
$env:DATABASE_URL = "postgresql://mdm_app:CHANGE_ME@localhost:5432/mdm_runbook"
python setup_rls.py verify
```

Pass: a check mark saying the role is not a superuser, does not bypass row-level security and does not own the tables. A cross mark means you are still connected as an owner or superuser, and the policies are silently doing nothing.

### 9c: Prove tenants cannot see each other

`verify` does not prove this; its own output says so. The engine sets `app.current_tenant_id` for each query, so set it by hand:

```powershell
psql $env:DATABASE_URL -c "BEGIN; SET LOCAL app.current_tenant_id = 'acme_bulk'; SELECT COUNT(*) FROM source_records;"
psql $env:DATABASE_URL -c "BEGIN; SET LOCAL app.current_tenant_id = 'acme_regular'; SELECT COUNT(*) FROM source_records;"
psql $env:DATABASE_URL -c "SELECT COUNT(*) FROM source_records;"
```

Expect 200 for each of the first two, and 0 for the third (a connection with no tenant set sees no rows). Anything else is a finding: send me the three numbers.

### 9d: Run the engine as the restricted role

The owner creates and changes the schema; the restricted role only reads and writes data, and the app only verifies the schema:

```powershell
$env:MDM_AUTO_MIGRATE = "false"
python migrate.py status
python bulk_ingest.py --tenant acme_onboard --domain customer --file samples/sample_customers_200.csv --mapping "$MAP" --id-column CustomerID --source-system crm --bulk --delta
```

Pass: `status` exits 0, and the load prints `Already held, unchanged: 200   |   changed, new version: 0`. If a query returns nothing under the restricted role that returned rows as the owner, that is a store method that does not set the tenant: send me the error.

### 9e: Roll back

Set `DATABASE_URL` back to the `postgres` user and run `python setup_rls.py disable`. Remove the `MDM_AUTO_MIGRATE` variable.

## Step 10 (optional): Start the API

The API and its review page are the user-facing layer; this step checks that they start on Postgres and answer. `validate_setup.py` in step 5 already exercises the HTTP layer and the access rules in a throwaway server, so this is a manual look.

Use the `postgres` connection (undo step 9's `DATABASE_URL` change), then:

```powershell
python seed_demo.py
$env:MDM_DEV_MODE = "1"
uvicorn app.api:app --port 8000
```

`seed_demo.py` loads demo data for the tenant `default`. Open `http://localhost:8000/` for the review page and `http://localhost:8000/docs` for the endpoint list. Authentication is required unless you set MDM\_DEV\_MODE=1, which turns it off for a laptop: no key, full administrator rights, tenant taken from the X-Tenant-Id header (default). The console has no sign-in yet, so it only works in that mode.

Pass: both pages load, the review queue shows items, and the uvicorn console prints no errors.

To try API keys, stop the server, then:

```powershell
Remove-Item Env:MDM_DEV_MODE
python manage_api_keys.py create --tenant acme_bulk --role tenant_admin --label "root key"
uvicorn app.api:app --port 8000
```

Copy the key it prints, and in a second window:

```powershell
curl.exe -i http://localhost:8000/v1/customer/review-queue
curl.exe -H "Authorization: Bearer <key>" http://localhost:8000/v1/customer/review-queue
```

Pass: the first call returns 401; the second returns the review queue for `acme_bulk` as a page, `{"items": [...], "next_cursor": null, "limit": 50, "total": 3}` (3 pairs after step 7c).

Then check the API over real HTTP, against this server on Postgres. Stop it, start it again with every merge flagged for audit (so the audit checks have something to look at), and run the script with the key you just made. It writes keys and records into `acme_bulk`, so use a scratch tenant:

```powershell
$env:MDM_AUDIT_RATE = "1"
uvicorn app.api:app --port 8000
# in a second window:
python validate_api.py --base http://localhost:8000 --admin-key <key> --confirm-writes
```

Pass: no `FAIL` lines (skips are fine). It checks that nothing but `/healthz` answers without a key, that roles and domain-limited keys are enforced with 403 (not 500), that a decision is recorded under the key's name, that audits and unmerge work, that the review queue and entity list return pages that add up exactly, and that oversized requests are refused. Run with no arguments instead, it starts its own SQLite servers: that tests the web layer but not Postgres. Remove `MDM_AUDIT_RATE` when you are done.

## Step 11 (optional): Gate P, the matching kernel on your database

This measures how many records per second ONE worker can resolve when the new matching kernel keeps its state in your Postgres. It is an experiment (`experiments/gate_p/`), not the product: the application still uses its existing engines, and nothing in steps 1 to 10 depends on it. It creates its own tables in a schema called `mdm_gate_p` and removes only its own rows afterwards; your tables are never touched. It is the number that decides whether the 100-million-record plan is reachable, and it can only be measured on a real server.

### 11a: Connect and check

```powershell
psql -U postgres -h localhost -c "CREATE DATABASE mdm_gatep;"
$env:GATE_P_DATABASE_URL = "postgresql://postgres:YOUR_PASSWORD@localhost:5432/mdm_gatep"
python experiments/gate_p/gate_p.py --check
```

Pass: it prints the environment, then `insert / read / IN-list / delete work; round trip for a trivial query: {...}`, then `OK: the real run can start`. The Postgres adapter (`PostgresDb` in `experiments/gate_p/dbs.py`) has never touched a server: if this fails, the traceback is the most useful thing you can send me. Run it from a machine as near the database as the real load will be; the round trip it prints is part of the answer.

### 11b: The three measurements

```powershell
python experiments/gate_p/gate_p.py --records 100000 --out gate_p_100k.json
python experiments/gate_p/gate_p.py --records 200000 --modes hot --cache-groups 20000 --cache-buckets 60000 --cache-records 60000 --out gate_p_small_cache.json
python experiments/gate_p/gate_p.py --records 50000 --modes hot --workers 4 --out gate_p_workers.json
```

The first compares three modes (naive, cache only, cache plus batched writes). The second is the one that matters: at 100 million records nothing fits in memory, so it runs with a cache far smaller than the data, and you should look at the rate by window, which on SQLite kept falling through the run. The third runs four workers, each with its own tenant, against the same database. Optional: `--records 5000 --verify` checks that the database-backed kernel gives exactly the in-memory kernel's groups and events on your server.

For comparison only, on SQLite (one core, no network): naive 194 records/s with 10.0 statements per record; cache plus batched writes 741 records/s with 0.78; with a cache a seventh of the data the rate fell from 834 to 457 records/s across the run. Send me the JSON files, and the traceback of anything that fails.

## Step 12 (optional): The read-only MCP server and the steward agent role

This checks, over real HTTP and on the real FastAPI, what I could only check by calling the endpoint functions directly: the new evidence and record-history endpoints, the read-only `steward_agent` role, and the MCP server. I could not run any of it against a real API or a real MCP client, so a failure here is a finding, not your mistake. It needs steps 6 and 10: the API running with authentication on, and tenant `acme_bulk` with its pending pairs from step 6 (3 after step 7c).

### 12a: The validator

```powershell
python validate_api.py
```

It starts its own servers (SQLite) and now includes section 7b, which makes a key with the role `steward_agent` and checks that it reads the queue, the evidence, a record's history and the audit sample, and is refused with a 403 on twelve endpoints that would change or administer something. Pass: no `FAIL` line in 7b. Against your Postgres, the `--base` command at the end of step 10 runs 7b too.

### 12b: Issue an agent key and start the server

```powershell
python manage_api_keys.py create --tenant acme_bulk --role steward_agent --label "agent-1" --domains customer
$env:MDM_API_URL = "http://127.0.0.1:8000"
python mcp_server.py --http --port 8765
```

The key is printed once; copy it. The API from step 10 must be running with authentication on. The MCP server listens on 127.0.0.1 only.

### 12c: Call it

In a second window:

```powershell
'{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | Set-Content -Encoding ascii tools_list.json
curl.exe -s -X POST http://127.0.0.1:8765/mcp -H "Authorization: Bearer <agent key>" -H "Content-Type: application/json" --data-binary "@tools_list.json"
'{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_review_queue","arguments":{"domain":"customer","limit":3}}}' | Set-Content -Encoding ascii queue.json
curl.exe -s -X POST http://127.0.0.1:8765/mcp -H "Authorization: Bearer <agent key>" -H "Content-Type: application/json" --data-binary "@queue.json"
```

Expect: the first answer lists 11 tools (all read-only); the second has `"isError":false` and a text that starts with `DATA from the platform`, holding the pending pairs of `acme_bulk`. Take a `candidate_id` from it and ask for its evidence:

```powershell
'{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_pair_evidence","arguments":{"domain":"customer","candidate_id":"<candidate_id>"}}}' | Set-Content -Encoding ascii evidence.json
curl.exe -s -X POST http://127.0.0.1:8765/mcp -H "Authorization: Bearer <agent key>" -H "Content-Type: application/json" --data-binary "@evidence.json"
```

Expect `group_a` and `group_b` (each with its members and golden record), `separations`, `history` and `other_pairs_of_record_a`.

### 12d: What it must refuse

Send each of these the same way. Pass: each is refused as described, and the review item is still pending afterwards.

- A tool that does not exist: `"params":{"name":"resolve_review_item","arguments":{}}` gives an error with code -32602 and the words `unknown tool`.
- An argument naming a tenant: `"arguments":{"domain":"customer","tenant_id":"x"}` for `get_review_queue` gives -32602 and `unexpected argument 'tenant_id'`.
- The API itself, asked directly with the agent key to resolve an item (put `{"decision":"confirm"}` in `decision.json`):

```powershell
curl.exe -s -o NUL -w "%{http_code}" -X POST http://localhost:8000/v1/customer/review-queue/<candidate_id>/resolve -H "Authorization: Bearer <agent key>" -H "Content-Type: application/json" --data-binary "@decision.json"
```

This must print `403`. Send me anything that differs, with the full response. Then stop the MCP server and remove `tools_list.json`, `queue.json`, `evidence.json` and `decision.json`.

### 12e: Let the agent record a recommendation (optional, off by default)

Stop the MCP server and start it again with the one write switched on. The key still needs the permission: the role `steward_agent` has it, a data steward does not.

```powershell
python mcp_server.py --http --port 8765 --allow-propose
```

The tool list now has 13 tools (the two new ones are `get_recommendations` and `propose_decision`). Record a recommendation for the item from 12c, using its `candidate_id`:

```powershell
'{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"propose_decision","arguments":{"domain":"customer","candidate_id":"<candidate_id>","recommendation":"confirm","confidence":0.9,"reasons":["same person"],"model":"test","prompt_version":"v1"}}}' | Set-Content -Encoding ascii propose.json
curl.exe -s -X POST http://127.0.0.1:8765/mcp -H "Authorization: Bearer <agent key>" -H "Content-Type: application/json" --data-binary "@propose.json"
```

Expect `"isError":false` and a result whose `agent` is `agent:agent-1` (the key's name) and whose `outcome` is null. The item must still be pending: check that the review queue's `total` has not changed. Then decide the item yourself with a data steward's key (or the console), and ask again with `get_recommendations`: the recommendation now has `"outcome":"agreed"` (you confirmed) or `"disagreed"` (you rejected), and `outcome_by` is your key's name. `GET /v1/customer/recommendations/stats` with any key shows the agreement counts. Without `--allow-propose`, `propose_decision` is an unknown tool (error -32602). Remove `propose.json` when done.

## Step 13 (optional): Replay your stewards' decisions through an agent

This asks how an agent would have done on review items your people already decided. It only reads: it writes nothing to the database. It needs human decisions to exist, so it is most useful on real data; on the sample data of steps 6 and 7 there are few or none, and the report will say so.

```powershell
python replay_decisions.py --tenant acme_bulk --domain customer --agent rule --limit 100 --out replay_results.jsonl
```

Expect a first line starting `READ-ONLY`, then a report that begins `Replayed N decided review items`. `N` is the number of review items a person decided (0 is possible on the sample data). Run it twice: the second run says that all of them are already in `replay_results.jsonl` and asks for nothing again. `python replay_decisions.py --report-only replay_results.jsonl` prints the report without a database. Nothing about the database should change: if you can, compare a dump before and after. The `rule` agent is only a baseline and will say needs_human for most items; to test a real agent see `docs/replay.md` (`--agent command`, or `--agent llm`, which sends each item's evidence to a model's API and refuses to run without `--yes`). Send me any traceback, and the first 20 lines of the report. Remove `replay_results.jsonl` when done.

## Step 14 (optional): Run our own agent in shadow mode on a few items

This exercises `agent_runner.py`: it asks a model about the review items waiting for a person and **records** a recommendation for each. It never decides anything: you still resolve every item. It needs the API from step 10 running with authentication on, and a few waiting items (`acme_bulk` has 3 after step 7c). **Each item's evidence is sent to the model provider's API (protected fields are masked, but names, emails and addresses are not): use the sample data, not real customers, for this first run.**

First look without spending anything. A key whose role is `steward_agent` (the runner holds nothing else), then a dry run:

```powershell
python manage_api_keys.py create --tenant acme_bulk --role steward_agent --label "runner-1" --domains customer
$env:MDM_API_URL = "http://127.0.0.1:8000"
$env:MDM_API_KEY = "<the key printed above>"
python agent_runner.py --domain customer --dry-run
```

Expect `DRY RUN: would ask about 3 items (N characters of evidence in all). Nothing was sent to a model and nothing was recorded.` Then a small real run, with your own model and key (there is no default model):

```powershell
$env:ANTHROPIC_API_KEY = "<your model provider key>"
python agent_runner.py --domain customer --model <a model your account can use> --yes --max-items 3 --max-per-day 20
```

Expect `recommended 3` (or fewer, with an `errors` count if the model or key is wrong; with errors it stops by itself after 10). Check three things. The items are still waiting: the review queue's `total` has not changed. The recommendations exist: `curl.exe -s -H "Authorization: Bearer <agent key>" "http://127.0.0.1:8000/v1/customer/recommendations?limit=5"` lists them, each with `"agent":"agent:runner-1"` and the model you named. Run the same command again: it should say `already done 3` and make no model call.

Then the kill switch: create an empty file named `agent_runner.STOP` in the folder, run the command again, and expect `STOPPED (switch)` with no model call. Delete the file afterwards. Finally decide one of the waiting items yourself (a data steward key, or the console) and read `GET /v1/customer/recommendations/stats`: the recommendation for it now has an outcome, `agreed` or `disagreed`. Send me the output of each run and the first recommendation, with its reasons. Remove `agent_runner_state.json` when done.

**See it in the console.** The console has no sign-in yet, so it only works in development mode, as the demo tenant `default` (step 10): start the API as in step 10 (`$env:MDM_DEV_MODE = "1"`, with the demo data from `seed_demo.py`), and run the runner against it. Development mode ignores keys and gives the runner full rights, so this look exercises the panel, not the permissions; any text will do as `MDM_API_KEY`. Let it record recommendations for the demo data (`python agent_runner.py --domain customer --model <a model your account can use> --yes --max-items 3 --max-per-day 20`). Then open `http://localhost:8000/`. Each review card that has a recommendation shows a line `Assistant's suggestion · available · advisory`, **closed**. Open one: it should show what the assistant suggests, how sure it is, its reasons, and which model wrote it, with a note that you decide. Leave another closed. Decide the one you opened and the one you did not, then read `GET /v1/customer/recommendations/stats`: `by_viewing` should show one decision under `decided_after_viewing` and one under `decided_blind`. That split is the point: only the blind figure measures the assistant without it influencing you. The panel has never been opened in a real browser, so tell me anything that looks wrong or is awkward, and send a screenshot of an open panel and the `by_viewing` block.

## Step 15 (optional): Try the key filter on a load

The key filter skips the blocking lookups that would find nothing: on 4,000 people, 95% of keys were never looked up and a record's statements fell from 8.0 to 6.2. On PostgreSQL each skipped lookup is a network round trip saved, so this is where it should matter. **It is only valid when nothing else writes the tenant and domain while the load runs** (no API traffic, no second loader). Use the variables of step 8 (`$PMAP`, the domain templates and the token key). Load the same 50,000 people into a new tenant with the flag (step 8b is the same load without it, so you may already have its time):

```powershell
python bulk_ingest.py --tenant people50k_filtered --domain person --file people_50000.csv --mapping "$PMAP" --id-column PersonID --source-system crm --bulk --key-filter
```

Expect, at the start, `key filter ON: 0 keys already in the index loaded; N MB for 50,000 new records` and `YOU ARE PROMISING that nothing else writes tenant 'people50k_filtered' domain 'person' until this load is finished.` At the end a line `key filter: ... (9x%) known to be held by no other record and not looked up`. The result must match step 8b: the same number of entities (46,650) and automatic merges (3,350); the number of review pairs must match too: loads are reproducible (ties are settled by what a record is, never by a random id), so the same file gives the same groups and the same pairs in every run. Send me `Done: 50000 records in N s` for this load and for step 8b, and the key filter line: the difference is what the filter is worth on your server. Loading a second file into a tenant that already has data also works (the filter reads the existing index first); with more than 200 million keys already there it says it was not used and loads normally.

## When something fails

Stop at the first failing step and send me the output. Most first-run failures are one of these:

| What you see | Likely cause | What to do |
| --- | --- | --- |
| `No module named 'psycopg2'` | The driver is not in this virtual environment | Activate `.venv`, then `pip install psycopg2-binary` |
| `pip` cannot install `psycopg2-binary` | Possibly no prebuilt wheel for Python 3.14 (I do not know) | Make the environment with Python 3.12 or 3.13 |
| `password authentication failed`, `database does not exist`, `could not connect` | Wrong URL, a password character not URL-encoded, or the server is not running | Test the same URL with `psql`; encode `@` as `%40` |
| `permission denied for schema public` | PostgreSQL 15 and later do not let ordinary roles create objects in `public` | Run migrations as the owner or superuser, or `GRANT CREATE ON SCHEMA public TO <role>;` |
| Step 5 reports skipped tests | `TEST_DATABASE_URL` is not set in this shell, or `psycopg2` is missing | Set it in the same window; check `python -c "import psycopg2"` |
| `migrate.py status` exits 1 after `upgrade` | A migration failed part way | Send the full `upgrade` output and the `status` output |
| `relation "entity_customer" does not exist` | The golden table has not been created | `python domains_cli.py reindex customer` |
| A message that the index is stale | The domain configuration changed | `python domains_cli.py status`, then `reindex <domain>` |
| `Error: tenant ... already has ... records` | Expected from --legacy-bulk only: the set-based engine refuses a tenant that has data (the default engine does not) | Use a new tenant, or add `--onboard` (new records) or `--delta` (changed records) |
| `MDM_TOKEN_KEY is not set` or `too short` | The person domain hashes its SSN and needs a key of 32 characters or more | Set `MDM_TOKEN_KEY` |
| `YAML config needs PyYAML` | `pyyaml` is not installed | `pip install pyyaml` |
| Loads are very slow | Network latency to Postgres (the default engine makes about 25 small store calls per record, in one transaction per 200 records), or, with --legacy-bulk, its temporary work files on a slow disk | Use `--workdir` on a fast local disk (--legacy-bulk only), and send me the timing |

What to send me:

- The step number and the exact command, with any password removed.
- The full output, including the whole traceback.
- The result of `SELECT version();` and of `pip list`, filtered to `psycopg`.
- The output of `python migrate.py status`.
- For a number that does not match: the query results next to the expected table.

## Clean up

Stop `uvicorn` first; a database cannot be dropped while something is connected to it.

```powershell
Remove-Item Env:DATABASE_URL, Env:TEST_DATABASE_URL -ErrorAction SilentlyContinue
psql -U postgres -h localhost -c "DROP DATABASE mdm_runbook;"
psql -U postgres -h localhost -c "DROP DATABASE mdm_test;"
psql -U postgres -h localhost -c "DROP ROLE IF EXISTS mdm_app;"
Remove-Item part1.csv, part2.csv, people_4000.csv, people_50000.csv -ErrorAction SilentlyContinue
```

The set-based engine's (--legacy-bulk) temporary work files are removed automatically when a load finishes.

## Reference

### Environment variables

| Variable | What it does | Needed for |
| --- | --- | --- |
| `DATABASE_URL` | The target database: a SQLite path or a `postgresql://` URL. Default: the SQLite file `mdm.db` | Every Postgres step |
| `TEST_DATABASE_URL` | The database for `tests/test_postgres_backend.py` and `validate_setup.py` section 5 | Step 5 |
| `MDM_AUTO_MIGRATE` | `false` makes the app only verify the schema; run `migrate.py upgrade` first as the owner | Step 9 |
| `MDM_TOKEN_KEY` | At least 32 characters; hashes protected fields such as the person domain's SSN | Step 8 |
| `MDM_TOKEN_KEY_ID` | A label for the key (default `v1`); change it with the key when rotating | Key rotation |
| `MDM_DOMAINS_DIR` | A folder of domain files to register (the project ships `domain_templates`) | Step 8 |
| `MDM_DEV_MODE` | `1` turns authentication OFF (laptop and demo only: no key, full rights, tenant from the header). Without it every request needs `Authorization: Bearer <key>` | Step 10 |
| `ANTHROPIC_API_KEY` | Lets the engine ask a language model to judge ambiguous pairs | Optional |
| `MDM_LLM_IN_BATCH` | Allows that model in batch loads (off by default) | Optional |
| MDM\_SIMILARITY\_CACHE\_SIZE | Entries in the string-similarity cache (default 131072, about 35 MB per process); 0 turns it off. Results never change, only speed | Optional |
| MDM\_EMBEDDING\_CACHE\_SIZE | Entries in the text-embedding cache (default 16384, about 35 MB per process); 0 turns it off | Optional |
| GATE\_P\_DATABASE\_URL | The database the Gate P benchmark runs against (sqlite:///path or postgresql://...), instead of --db | Step 11 |
| MDM_ENGINE | kernel (default), or legacy: the older pairwise engine, for rollback and comparison | Optional |
| MDM_MERGE_GROUPS | auto (default), propose or off: what to do when two groups look like one entity | Optional |
| MDM_MAX_CANDIDATES | A number: score only the best N groups per record (default 0 = all); a speed dial that costs accuracy | Optional |
| MDM_RANK | idf (default) or keys: how groups are ranked when that limit is set | Optional |
| MDM_KERNEL_SCORER | corroborated (default), frequency or rule | Optional |
| MDM_REVIEW_CAP | A number (default 5): review items for one record | Optional |

### Scripts used

| Script | What for |
| --- | --- |
| `migrate.py` | Show and apply schema migrations (`status`, `upgrade`, `upgrade --dry-run`) |
| `bulk_ingest.py` | Load a CSV or JSON file with the default engine (the kernel): a normal load, or `--bulk` (the same code path; `--onboard` and `--delta` are accepted but not needed). `--legacy-bulk` selects the set-based pair engine; `--workers`, `--workdir`, `--dry-run`, `--resume`, `--budget` and `--lineage` belong to it. |
| `domains_cli.py` | Domain tools: `list`, `status`, `reindex`, `profile`, `process-pending`, `schema` |
| `setup_rls.py` | Row-level security: `create-role`, `enable`, `verify`, `disable` |
| `validate_setup.py` | One command that runs the "never tested here" paths and prints PASS, FAIL or SKIP |
| validate\_api.py | Checks the API over real HTTP: authentication, roles and domain scope, signed decisions, audits and unmerge, pages, limits |
| `manage_api_keys.py` | Create, list and revoke API keys directly in the database |
| `mcp_server.py` | The read-only MCP server for AI agents: stdio, or HTTP with --http (step 12; docs/mcp-server.md in the zip) |
| `replay_decisions.py` | Replay decided review items through an agent and report how often it would have agreed with the people (step 13; docs/replay.md in the zip). Read-only |
| `agent_runner.py` | Our own stewardship agent in shadow mode: asks a model about waiting review items and records recommendations; decides nothing (step 14; docs/agent-runner.md in the zip) |
| `seed_demo.py` | Load demo data for the tenant `default` |
| `samples/make_people_csv.py` | Write any number of synthetic people as a CSV |
| experiments/gate\_p/gate\_p.py | Gate P: runs the matching kernel on a database in three modes and reports records per second, statements per record, latency, cache hit rates and a projection to your target (step 11) |

### Expected numbers at a glance

| Load | Records | Active entities | Candidates |
| --- | --- | --- | --- |
| Step 6, 200 customers (--bulk, and without it: the same engine) | 200 | 165 | confirmed 35, pending 1 |
| Step 7a, the same file in two loads | 200 | 165 | confirmed 35, pending 1 |
| Step 7c, after the changing delta | 200 | 165 (26 merged) | confirmed 35, pending 3, superseded 42 |
| Step 8a, 4,000 people | 4,000 | 3,744 | confirmed 256, pending 61 |
| Step 8b, 50,000 people | 50,000 | 46,650 | confirmed 3,350, pending 745 |
