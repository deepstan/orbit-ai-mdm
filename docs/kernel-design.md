# The matching kernel: design

Status (2026-10-14): **the application runs this design as its matching engine** (section 0). The prototype that measured it is `experiments/kernel_v2.py`; sections 1 to 11 are the design
and the evidence, and say in each place what has and has not been measured. Where this document says "the prototype" it means that experiment; where it says "the application" it means `app/`.

## 0. As built: the kernel is the application's matching engine

**What was replaced.** One function: `pipeline._route_record`, the step that answers "which entity does this record belong to?". It was a pairwise step (every candidate record scored against the
new one, a pair saved for each). It is now `app/kernel_router.py` (the database part) over `app/kernel.py` (the pure part: scorers, how a group looks to the matcher). Everything around the step is
unchanged and is shared by every way a record can arrive: landing, staging, the processing states and resume, history of changed records, golden-record publishing, runs, the review queue, a steward's
confirm and reject, unmerge, audits, lineage, the learned scorer. The old step is kept, selectable with `MDM_ENGINE=legacy`, for rollback and for comparison.

**No new tables and no migration.** A group is built on demand from what the product already stores: the entity's records, their staged forms, the blocking index. So the kernel always sees current
membership: a steward's confirm, an unmerge, a purge or a legacy bulk load cannot leave it stale, and data made by either engine can be continued by the other (a test proves it). This is why
section 5's tables are NOT needed for the first integration; they remain the design for the persistent, batched version (section 7c).

| Design (sections 2 to 4) | As built in the application |
|---|---|
| a group | an entity: its `entity_<domain>` row and the records whose `entity_id` it is; pseudo-groups for a record with no entity yet (an interrupted run) |
| the group's feature set | rebuilt from the members' staged forms on every use (no stored index); a record is compared with the group's value NEAREST to its own for each field |
| golden record as a derived view | survivorship as before; the golden record's MATCHING form is taken from the member that wins each field, not by standardizing the golden values again (a golden value can be typed or hashed) |
| candidate retrieval | the existing record-level blocking index, mapped to entities; groups ranked by rarity-weighted shared keys; `MDM_MAX_CANDIDATES` limits how many are scored (default: all) |
| value-frequency table | counted over the candidate records of each arrival (not stored): an identifier held by 3 OTHER groups is capped at 0.80 unless the names corroborate |
| scorer | `CorroboratedScorer` by default (the frequency rule, and a changed surname does not undo a certain match); `MDM_KERNEL_SCORER=frequency\|rule` selects the others |
| join | the record joins the best group scoring auto-merge, with ONE confirmed pair to the group's closest member: an entity is a TREE of pairs, so unmerge always has one pair to remove |
| two groups that are one | a record that matched two groups strongly joins the better; if the two groups' golden records match each other they are merged (one `group_merge` pair; Senzing and Reltio do this), otherwise the other group is queued for a person |
| a possible match | pending pairs for a person (at most `MDM_REVIEW_CAP`, default 5); the record is still an entity of its own, as before |
| update (decided by the owner: as Senzing and Reltio) | the changed record is matched again like a new one, so it can LEAVE its group (its system pairs are superseded, the members who stay are kept connected, both golden records are rebuilt) or move to another; **a record a person tied to its group never leaves** |
| human decisions are constraints | a pair a person separated is never proposed again and is never the "evidence" pair of a review; two entities with a separation between them are never merged automatically; the old wording of the reason is kept |
| learned scorer, audit sampling, LLM adjudication, blending with the learned model | the same functions as before, called at the same points |
| relationships (households), event outbox, `anchored` update policy | **NOT in the application**: prototype only |

**Settings** (environment; a bad value stops the program with a message, it is never ignored):

| Variable | Values | Default | |
|---|---|---|---|
| `MDM_ENGINE` | `kernel`, `legacy` | `kernel` | `legacy` is the old pairwise step |
| `MDM_MERGE_GROUPS` | `auto`, `propose`, `off` | `auto` | `propose`: the groups are queued for a person instead of merged (Ataccama); `off`: neither |
| `MDM_MAX_CANDIDATES` | a number, 0 = all | all | a speed dial: scoring only the best N groups costs accuracy that grows with the data (section 7d) |
| `MDM_RANK` | `idf`, `keys` | `idf` | how groups are ranked when the limit is set |
| `MDM_KERNEL_SCORER` | `corroborated`, `frequency`, `rule` | `corroborated` | |
| `MDM_REVIEW_CAP` | a number | 5 | review items for one record |
| `MDM_BATCH_COMMIT` | a number | 200 | records matched per transaction in a batch run, and the most a dying process can lose |

**What changed for a user.** (1) A changed record can leave its group (before: it never could). (2) Fewer pair rows: a join saves one pair, not one per similar record, so the `match_candidates` counts
are smaller than the legacy engine's for the same data (the people benchmark's auto-merged count is a count of tree edges now). (3) `stayed: true` in the result of a changed record that still belongs
where it was (it is not reported as merged: it was already there). (4) `bulk_ingest.py --bulk` is the same code path as a normal load; the set-based engine is `--legacy-bulk`, and its own
options (`--workers`, `--workdir`, `--dry-run`, `--resume`, `--budget`, `--lineage`) are refused with `--bulk` instead of being ignored.

**Measured, on the application (SQLite, one core).** 200 customers: 165 entities, 35 automatic merges, 1 review item, **the same as the legacy engine**, through either loader path. 4,000 people: 3,744
entities (legacy 3,743), 256 confirmed pairs, 61 pending (legacy 65 pending). **Speed, before and after the writes were batched:** 4,000 people 50.8 s (79 records/s) before, **5.3 s (748/s)** after; 50,000 people about 10.5 minutes before, **about 101 s (490/s on average)** after, the same 46,650 entities and 3,350 confirmed and 745 pending pairs; the set-based engine takes 51 s. The rate falls with the data (654/s over the first 10,000 records, about 420/s over the last 25,000): Why it falls (investigated 2026-10-15): the SQL time per record rises from 0.70 ms on an empty database to 2.10 ms at 50,000 records while the number of calls per record stays flat; the growth is in COMMIT (+0.59 ms per record), the insert into blocking_keys (+0.26), marking records matched (+0.24) and the lineage insert (+0.15). Ruled out as the main cause by experiment: the transaction size (200, 1,000 and 2,000 records per commit: the same speed), SQLite's cache size and WAL (inside the +-10% run-to-run noise), skipping lineage (about +14%, noisy), and keying the phone exactly instead of with every adjacent-digit swap (the key table shrinks 46%, +5%, and on this synthetic data the same entities, the same 3,350 confirmed pairs and 744 against 745 pending). So the cause is only partly explained; `experiments/sql_time_by_statement.py` is the tool to rerun it, and the next measurement belongs on PostgreSQL. **Profile of 1,500 people, before:** SQLite commits 40% (12.6 per record), SQLite statements 33%, a new connection per store call 7%, scoring 3%, the kernel's own decision logic under 1%. **After:** 12 commits in all (commits 0.1%), statements 19%, scoring, similarity and standardizing 18%, the kernel's own logic 3%, and about 60% Python overhead (row parsing, JSON, dataclasses, store code). So the slowdown against the set-based engine was never the kernel: it was the store's one-commit-per-call pattern, which is what Gate P measured and removed in the prototype.

**Does the integrated engine keep the prototype's accuracy?** `experiments/app_engine_benchmark.py` runs the hard-cases benchmark (with life-event updates) on the APPLICATION's engine, kernel against legacy, two seeds: precision 0.994 against 0.941, recall 0.896 against 0.906, F1 0.942 against 0.923; wrong merges after an update that turns a record into someone else 0 against 36. The prototype measured F1 0.946; the small gap is probably the integration's use of the existing record-level blocking index and its way of deriving a group's golden values, not confirmed. Where the kernel is weaker: it is more cautious, so after a life event it missed 6 same-person pairs against 1 (the old engine never splits a record from its group). On the 4,000-person file the kernel and the set-based engine put the same 267 pairs together; the set-based engine also merged 2 more (by chaining) and the kernel none it did not. **Neither is "correct" in an absolute sense:** the truth is known only in a benchmark I generated, and I wrote some of the kernel's rules after seeing its failures, so its score is optimistic. Which is better depends on what a wrong merge costs against a missed one; a steward labelling the disagreements on real data would settle it.

**Batching the writes (built, 2026-10-15).** `store.batch()` is one transaction for everything its block writes (reentrant; per thread on SQLite, and on PostgreSQL it holds the store's one connection); `store.savepoint()` undoes one block inside it. `run_batch` and `process_pending` first LAND the records (raw and staged: one commit, durable before any matching), then match them in transactions of `MDM_BATCH_COMMIT` records (default 200); each record's matching runs under a savepoint, so a failing record rolls back only itself and is recorded as unfinished with its error. A single realtime `ingest_record` is the same unit of work with one record (landing, then one transaction: 4 connections instead of about 25; a batch of 200 records opens 9 instead of about 5,000). **What it changes:** matching is ATOMIC. A process that dies loses at most the matching of the batch in flight (up to 200 records; landing is durable), nothing is left half done (no pair without its merge), and `process-pending`, or running the command again, finishes with the groups an uninterrupted run gives (tested at several batch sizes and crash points). The tests that asserted partial durable states (a pair saved but not merged) were rewritten to assert that nothing half-done survives, and the replay of a pair saved without its merge is still tested by building that state by hand. **What it cost to get right:** creating or checking a golden table goes through the migration runner, which opens its OWN connection and would wait for the batch's write lock for ever (the first probe hung on it): the check is now made before the transaction opens (`store.batch(domain=...)`), and inside a batch it raises a message saying so. **Not yet run on PostgreSQL**, where every statement is also a network round trip.

**Fewer statements (built, 2026-10-15).** A record in a batch cost 16.8 SQL statements (person) and now costs 8.0 (customers 12.7; realtime 22.8 with landing and the index checks): a new one-record entity is written once with its golden record, computed from the record already in memory (it took six statements); lineage rows are pooled and written once per transaction; the held-record lookup is one query per chunk (it was one per record, in both stores); a new record's blocking keys are inserted without a delete (`replace_blocking_keys_batch(fresh=True)`); separations are asked once per transaction in a batch. Same groups, pairs and golden records (all 3,744 identical, ids and timestamps normalised). **One behavior change:** an entity created and then changed by a merge inside one batch records both lineage steps (A, then A to B), as realtime always did; before, a batch recorded only the final value (30 of 3,744 entities). On SQLite the speed gain is small (+4% from scratch, up to +15% onto a large base, within the noise) because a statement is nearly free there; the saving is a network round trip each on PostgreSQL (8.8 per record), unmeasured. What remains of the 8 statements is mostly reads (blocking buckets and members, which groups the candidates belong to, the groups' records and staging): a hot cache for those is the next step.

**The key filter (built, 2026-10-19; off by default).** Of a record's 8.0 statements, 2.8 are reads and ~1.9 of those are the two blocking lookups, and **92% of records share none of their 14 keys with any other record** (4,000 people), so for them both lookups find only the record itself. A read cache cannot skip them (it cannot say "nothing is there" without holding the whole index, which at 100 million records is 1.4 billion keys); a **Bloom filter of the keys already indexed** can: "definitely never seen" means no lookup, "maybe" means ask as before (`app/keyfilter.py`, `bulk_ingest.py --key-filter`). Measured on 4,000 people: statements per record 8.04 to 6.21, reads 2.81 to 0.98, 95% of keys never looked up; groups, golden records and pairs identical. **It is only valid with ONE writer** (nothing else may write that tenant and domain during the load: a key added behind its back would be a lost candidate), it is loaded from the existing index first and gives up cleanly if that is too big, and a false "maybe" costs only a lookup. What is left of the 6.2 statements is mostly WRITES (5.2): the next lever is writing in larger groups or pipelining, which can only be tested on PostgreSQL. Not run on PostgreSQL. Writing it found that the engine was slightly non-deterministic: two identical loads could differ on a genuine tie, because ties were broken by random ids. **Fixed (2026-10-19):** every tie is now settled by what a record IS (its source system and source record id), candidate weights are rounded, and a tie across the candidate cut-off is resolved before cutting; identical loads in separate processes with different hash seeds now give identical groups, golden records and pairs (`tests/test_determinism.py`), so Gate S step 2 has a reproducible sequential run to compare with. One tie is still settled by an id: which of two entities survives a merge when both were created in the same microsecond.

**What was found by running the old tests against it** (27 of 855 failed at first): two real bugs, both fixed and both pinned by tests. (1) On resume, a record whose pair had been saved and whose entity
merge had been replayed was given a SECOND entity. (2) After an unmerge, the group holding the separated partner was queued for review through its closest member, which could be the very record the person had
separated: the pair a person said was not the same was proposed again. The other failures were tests of the legacy engines against each other (12, now pinned to `MDM_ENGINE=legacy`), tests that needed
two paths between records (3, now built the realistic way: a steward confirms a redundant pair), and tests that injected a crash into a lookup only the legacy step makes (7, now engine-neutral).

**Tests.** `tests/test_batch_writes.py` (18 tests, including the statement counts and the lineage guarantee: the same groups at every batch size and one at a time, a process dying mid-batch, one failing record, another thread never seeing a half-written batch, the connection counts; checked by breaking the code 8 ways, and the contract on both databases is in `tests/test_store_contract.py`); `tests/test_kernel_engine.py` (30 tests through the real entry points; 14 deliberate breakages of the engine were tried and every one is caught, after two test weaknesses were found and fixed);
the experiment's 55 tests, now against the SAME scorer code as the application; the existing suite.

## 1. What this must be, and the constraints

* **One matching strategy, not three engines.** Every record is compared with **entities** (groups), never with other records.
* A group has one golden record, built from its members by survivorship. A record that matches nothing starts a new group.
* **The system never regroups on its own.** Only a new arrival, an update or a person changes membership. Unmerge is a split. An **update** removes the record from its group (the golden
  is rebuilt without it) and re-matches the new version as a fresh arrival, so it can leave.
* **Scale:** an initial load or a new source of 100 million records in about 12 hours (2,315 records a second, sustained). **Delta performance is the priority** after that.
* **Quality:** at most 1 false merge in 2,000 automatic merges, kept honest by audits and an automatic rollback (this already exists and is unchanged).
* **At least on par with the main competitors at the design level** (review: Senzing, Tamr, Reltio, Informatica, Splink, AWS Entity Resolution).

## 2. The model

| Thing | What it is |
|---|---|
| **Group** | the entity: a set of source records. Its id is stable: when groups merge, the older id survives; when a person splits one, the side holding the oldest record keeps it. |
| **Feature set** | for each field, every distinct value the members hold and how many members hold it. This is what a record is matched against. |
| **Golden record** | a VIEW derived from the members by the existing survivorship rules. Stored for readers (it is the `entity_<domain>` table today), rebuilt when membership changes. Not what is matched. |
| **Value statistics** | which groups hold each identifier value (and how many). Finds identifiers that do not identify, and over-populated blocking keys. |
| **Link** | a typed relationship between two groups that are NOT the same entity (`household` today; `possible_match` is the review item). |
| **Review item** | a person's decision is needed: "this record may belong to that group", or "these two groups may be the same". |
| **Event** | one row per decision, written in the same transaction (a transactional outbox). Replaying the events rebuilds the groups and links exactly. |
| **Constraint** | a person's decision (confirm, reject, unmerge) is an INPUT to matching: must-link and cannot-link, enforced in one place. |

## 3. The operations

**Arrive(record).** Standardize. Look up candidate groups by blocking on each group's keys (the golden's, plus the alternates its feature set has seen), skipping any key held by
more than `cap` groups. For each candidate build the group's *view for this record* (the golden, except that each field takes the group's value nearest to the record's), and score it
with the scorer. Then:
1. a candidate scores `auto_merge` -> join the best; if a second is within `margin` (0.05) of it, ALSO queue "these two groups may be the same";
2. else if a candidate scores `review` -> start a new group and queue "this record may join that group";
3. else start a new group.
After joining, **compare the group's golden with the other auto-merge candidates' goldens and merge the groups whose goldens match each other** (entity to entity). Then, for candidates that
are not the same entity, apply the relationship rules (a household: same address and family name, different first name) and add a link. Every step appends events.

**Update(record).** Remove it from its group (rebuild that group, or delete it if empty), then match the new version. **DECIDED by the owner: do what Senzing and Reltio do**, which is to match the changed record
again as a fresh arrival against every group (`update_policy="rematch"`, the default), with NO special identity guard. Senzing re-resolves a changed record and may split an entity; Reltio re-runs cleanse, match and merge on
every update. Two things follow. (1) Our scorer must not let a changed SURNAME undo an otherwise certain match, or this policy splits real people after a marriage: `CorroboratedScorer` (the default scorer) gives the last-name
penalty back when two identifiers (email AND phone) and the first name agree. That rule is OURS: neither product documents it. (2) `update_policy="anchored"` (a record STAYS while it still shares an identifier with its group,
and a "kept after change" review is queued; modelled on Ataccama's identity stability) remains available as a setting for a customer who wants that guarantee, but it is not the default.
Measured over three seeds, ~780 records and ~65 updates each, with life-event updates (a new surname, or a move):

| update policy + scoring | real people split from their groups | F1 | precision | extra reviews |
|---|---|---|---|---|
| rematch, plain scoring | 38 of 72 | 0.930 | 0.996 | 0 |
| anchored, plain scoring | 4 of 72 | 0.949 | 0.996 | 34 |
| **rematch, `CorroboratedScorer` (the default)** | **8 of 72** | **0.946** | **0.996** | **0** |
| anchored, `CorroboratedScorer` | 4 of 72 | 0.949 | 0.996 | 4 |

No wrong merges were added: the household and Jr/Sr cases are unchanged. The 8 that remain are surname changes where only ONE identifier survives in the record, which the rule deliberately does not forgive.

**Delete(record).** The removal half of Update.

**Unmerge / split.** A person splits a group; each part gets a golden built from its own members; a cannot-link between the parts is recorded and enforced.

**Human decisions** are applied BEFORE scoring: a must-link joins, a cannot-link forbids, in the same place for every execution mode. (Today this is done three different ways.)

## 4. The five decisions, and the competitor evidence behind each

1. **Feature sets, golden as a view.** Reltio matches over all values of an attribute and materializes the golden record as a view; Senzing matches on features. Here: a group's second
   email, second phone or spelling variant finds it without being the survivor.
2. **Value statistics.** Senzing tracks attribute statistics as data arrives; Splink adjusts for term frequency (a rare value agreeing counts for more than a common one). Here: an exact match on an
   identifier that other groups hold is capped below auto-merge unless the names corroborate; a key shared by too many groups is not used to find candidates.
3. **Relationships as first-class links.** Senzing and Reltio model them. A household is a relationship, not a false merge and not nothing.
4. **An event outbox.** Reltio and Informatica publish change events; downstream systems and audits need them. Here the events are also the proof of consistency (replay equals state).
5. **A pluggable scorer.** `score(record, view, context) -> Result`. `RuleScorer` is today's scorer unchanged; `FrequencyAwareScorer` adds the identifier rule. A probabilistic scorer
   (Fellegi-Sunter weights learned without labels, with term frequency; per-field log-weights stay explainable) or the learned model can be dropped in.

## 5. Persistent data model (the DESIGN for the batched version; the application's first integration needs none of it, section 0)

`groups(id, tenant, domain, status, created_at)`; `group_members` is `source_records.entity_id` (unchanged); `group_features(group, field, value, n_records)`;
`group_keys(key, group)` (the blocking index, keyed to groups: smaller than today's per-record index); `value_holders(field, value, n_groups)` (maintained incrementally, or derived from `group_features`);
`links(a, b, type, evidence)`; `review_items`; `decision_events(seq, tenant, type, payload)`; human decisions stay in `match_candidates` rows (`unmerge:`, steward confirm/reject) and become constraints.
**Unchanged:** staging, standardization, survivorship and the golden tables, lineage, the hash-chained audit log, merge audits and the audit page, unmerge, the learned scorer's promotion hook,
tenant isolation and row-level security, migrations. Existing deployments migrate by treating each current entity as a group (members = its records) and re-deriving features; nothing is re-resolved until a group is touched.

## 6. Execution modes: one algorithm, different drivers

* **Real time (one arrival):** Arrive/Update as above, one transaction, events included.
* **Delta (a batch of new and changed records):** the same, in a canonical order (record id), with a lock per touched group.
* **Bulk (initial load, new source):** the SAME operation, run by many workers at once (section 7). Whether it reaches the 12-hour target is what Gate S decides.

## 7. Scale: ONE engine, many workers

**What "Gate S" means.** It is a go/no-go experiment ("S" for Scale), run BEFORE any scale machinery is built. The question it answers: *can the one engine, run by many workers at once, give nearly
the same answers as running it one record at a time, with few conflicts, and fast enough for 100 million records in 12 hours?* We simulate it first (cheap and deterministic), then try it with real
threads on Postgres, and only then build. A fail changes the plan before money is spent on it.

**Decision: there is one operation, `resolve(record)`, and a bulk load is that same operation run concurrently by many workers against the shared store.** There is no separate batch engine
and no separate bulk pipeline. This is how the entity-resolution specialists do it (reviewed in October 2026, from their own documentation):

| Vendor | Real time vs bulk |
|---|---|
| Senzing | one engine; a load is many threads, processes or machines calling the same add-record operation; input is shuffled so one entity's records do not arrive together; work deferred by contention becomes **redo records**, processed later by the same engine; consumers are told which entities were affected and must not process one entity in parallel |
| Reltio | one match engine, a bulk-job interface and a real-time API in front of it |
| Ataccama | one matching logic for full loads, incremental updates and real time |
| Informatica | a separate batch pipeline (load, tokenize, match, merge jobs) on one rule configuration; a batch is matched against the whole base, with a tunable batch size. **This is the pattern we are not following.** |
| Semarchy | a load is one unit through certification tasks; the same entity must not be in two queues |

**How concurrency is made safe (to be built and tested):**
1. every group carries a **version**; a resolution reads the groups it touches with their versions and commits only if none changed, otherwise it retries (optimistic concurrency);
2. input is **shuffled** (or arrives in canonical hash order), so one entity's records are rarely in flight together;
3. work that a concurrent change makes necessary but that need not block (a group merge that became possible because a golden changed meanwhile; a second look at an ambiguous candidate) goes to a **redo queue**, drained by the same operation;
4. downstream consumers read the event outbox and never process one group in parallel;
5. results need to be **close to** the sequential kernel's, not identical: sequential arrival order already changes at most a handful of record pairs (section 8); the redo queue is what drives convergence.

**Gate S, step 1: SIMULATED (2026-10-19; `docs/gate-s.md`).** On the benchmark (7,000 to 31,000 records, in memory) the gate passes: F1 within 0.085 points of sequential at up to 4,096 workers, and the redo queue drains (at most 3 tries). **But validating a read by a group VERSION, as the list above says, is the wrong validation**: 98 to 99% of its conflicts are false alarms, and at 4,096 workers it leaves about 500 effective workers. **Validate by RE-DECIDING instead** (reuse the scores of groups that did not change; a conflict only if the decision would change): conflicts 0.1 / 0.3 / 1.8 / 12.4% at 8 / 64 / 512 / 4,096 workers on 7,363 records and 3.2% at 4,096 on 30,835, so they fall as the data grows (roughly workers divided by records; an extrapolation, not a measurement). The candidate SET must be compared too (a phantom). **Not tested: hot groups** (the benchmark has no group over 20 members), a database, and the design's 1,000,000 records.

**What was measured, and a correction.** In memory (so no database cost), key cap 30, after fixing a bug in my prototype and caching two pure functions:

| records | ms / record | records / s / core | candidates scored / record |
|---|---|---|---|
| 772 | 1.5 | 654 | 10.5 |
| 13,000 | 3.1 | 321 | 28.8 |
| 26,000 | 3.3 | 304 | 29.7 |

* **CORRECTION.** An earlier version of this document said the cost per record "rises with the data" (3.9, 9.0, 12.8 ms; 78 records/s/core at 13,000 records) and called the kernel too slow. That was mostly my prototype, not the design:
  a rebuild scanned the whole value-holders table (an O(N) bug), and two pure functions were recomputed endlessly. Same 6,500-record run, identical groups: 58.3 s, then 45.1 s (bug fixed), then 26.1 s (string
  similarity cached: 96% of calls were hits), then 14.0 s (text embedding cached). **The cost per record is now flat, about 3.3 ms, so about 300 records/s/core in memory.**
* **Arithmetic for the target:** 100M in 12 hours is 2,315 records/s, so about 8 cores of CPU if it scaled perfectly and the database cost nothing. The database is the unknown (section 7c): it is not measured, because
  there is no Postgres server in the sandbox. The set-based bulk engine this replaces ran a 500,000-record delta in about 30 minutes on one core (282 records/s, before the later optimizations).
* **Independent components do not exist on this data**: joining records that share any blocking key (keys held by more than 30 records ignored), 99.7% of records fall in ONE component at 730, 6,000 and
  15,000 records. That rules out PARTITIONING a load into independent pieces. **It does not rule out concurrency on one shared store**, where a conflict needs two in-flight records to touch the same group.
  (The benchmark has small name pools; real data is more diverse and has heavy hub names. Measure, do not assume.)

**Gate S (before building any of this):**
1. *Simulation, two scheduling variants of the SAME code:* (a) W workers with optimistic commit and a redo queue (Senzing's pattern); (b) parallel scoring of records against a snapshot followed by a serialized assignment and review phase (Ataccama's pattern: its proposal phase is serialized; Informatica batch-matches this way too, but as a separate pipeline). Run W workers deterministically: waves of W records read the same snapshot, then commit with version checks, retries and a redo queue. Measure the conflict and redo rate for W = 8, 64, 512, 4096
   on the benchmark and on a synthetic file of at least 1M records; require F1 within 0.5 points of the sequential kernel and a redo queue that drains.
2. *Real threads on Postgres* (only if 1 passes): throughput against the number of workers, conflict rate under load, and a projection to 2,315 records/s on a stated number of cores.
3. *If the gate fails:* micro-batches run as a **driver of the same `resolve` function** (shared code, not a second engine), accepting looser order independence. Decided only on the evidence.

## 7c. Performance design options, and the recommended order

Where the time goes now (profile, 6,500 records): scoring candidates 47% (22.5 scored per arrival), building each group's comparison view 24%, rebuilding the golden 15%, standardization and key generation about 19%.

| | Option | Evidence | Expected effect |
|---|---|---|---|
| A1 | cache the pure functions (string similarity, text embedding); fix the O(N) rebuild scan | **DONE and in the application** (`app/perf_cache.py`): kernel 58.3 s to 14.0 s, identical groups; the EXISTING engines on 1,297 records, identical groups: bulk 7.1 s to 3.3 s (2.2x), one-at-a-time 55.0 s to 41.3 s (1.3x); hit rates 85% (similarity) and 99% (embedding) | 4.2x for the kernel |
| A2 | rank candidate groups by shared-key evidence and score only the top few | **MEASURED** (section 7d): rarity-weighted ('idf') ranking, top 3: IDENTICAL accuracy at about 730 records, 6.5x faster at cap 200; at 13,000 records every setting loses a little accuracy (top 6: 0.27 points, top 10: 0.13, top 16: 0.09) | 1.5x to 3x at 13,000 records for a stated, growing loss: a DIAL, calibrate per dataset |
| A3 | calibrate the key cap from the bucket-size distribution, not a constant | **MEASURED**: with ranking on, the cap barely matters (F1 0.9468 at caps 10, 30, 100 and 200); without ranking, cap 30 against 200 cut candidates 62 to 11 at identical accuracy | up to 3x unranked; default 30 |
| A4 | rebuild the golden incrementally; do not re-standardize it | profile: 15% + 7% | up to 1.3x |
| A5 | a native (compiled) string-similarity library | NOT measurable here (none installed, no network) | smaller after A1's 96% cache hits |
| B1 | **group commit**: one transaction per N arrivals, multi-row writes, a few statements per record amortized | **BUILT and measured on SQLite only** (section 7d): 10.0 statements per record naive, 0.78 hot; database share of the time 54% to 10%; NOT yet measured on PostgreSQL | likely the dominant lever on PostgreSQL, where a statement costs a network round trip |
| B2 | keep the hot working set (key index, feature sets, holders) in process memory with write-behind to Postgres; shard by key hash if it does not fit | **BUILT** (`experiments/gate_p/sqlstate.py`); measured on SQLite, including with a cache a seventh of the data: the rate keeps FALLING as the hit rate falls (834 to 457 records/s) | removes reads only while the cache holds the working set |
| B3 | partition tables by tenant and domain, and by key hash | design | parallel-friendly storage |
| C1 | declared partitions (never compared with each other) | Ataccama | free coarse parallelism |
| C2 | optimistic concurrency + redo queue, many workers on one store | Senzing; Gate S | the one-engine scale-out. **SIMULATED** (`docs/gate-s.md`): passes if a read is validated by re-deciding, not by a group version; step 2 (PostgreSQL, data with hubs) not run |
| C3 | snapshot scoring in parallel, then a serialized assignment phase | Ataccama; a scheduling mode of the same code | fallback inside Gate S |
| D1 | several worker PROCESSES (Python) | simple; each keeps its own caches | linear until the database saturates |
| D2 | compile the hot loop (Rust or C) | only if the profile is CPU-bound AFTER B | 10x or more on the CPU part; high cost |
| D3 | a cluster framework (Spark) | Tamr and Splink route; vendor claims only | only if Gate S fails |

**Order (approved by the owner):** (1) A1 in the application: DONE. (2) A3 and A2, measured: DONE (section 7d). (3) **Gate P (Persistence)**: the kernel on a database with B1 and B2, and real records per second
per worker: BUILT and verified on SQLite, **NOT YET RUN ON POSTGRESQL**: that number decides everything and only the owner's server can give it (`experiments/gate_p/README.md` says how). Pass = a projection to
2,315 records/s on no more cores than the owner has. (4) Gate S on top of the measured persistent kernel. (5) D2 only if still CPU-bound.

## 7d. Measured so far: ranking, the key cap, and the kernel on a database (SQLite as a PROXY)

**Ranking and the key cap** (in memory, caches on, relationships off; `experiments/sweep3.py`). Three seeds of about 730 records + 40 updates, 28 configurations:

| key cap | unranked: candidates scored, seconds | idf, top 3: candidates scored, seconds | F1 (all of these) |
|---|---|---|---|
| 10 | 6.2, 0.74 | 2.6, 0.39 | 0.9468 |
| 30 | 10.6, 0.88 | 2.7, 0.46 | 0.9468 |
| 100 | 40.5, 2.49 | 2.8, 0.48 | 0.9468 |
| 200 | 62.1, 2.85 | 2.8, 0.44 | 0.9468 |

The simple ranking by the NUMBER of shared keys loses accuracy as the cap rises (top 3 at cap 200: 0.9360), because a common key counts as much as a rare one; weighting by rarity does not. At larger sizes, one seed, cap 30:

| records | ranking | F1 | ms per record | records per second |
|---|---|---|---|---|
| 6,500 | unranked (cap 200) | 0.9408 | 5.37 | 186 |
| 6,500 | unranked | 0.9408 | 1.78 | 561 |
| 6,500 | idf top 3 / top 6 | 0.9387 / 0.9401 | 0.59 / 0.70 | 1,681 / 1,419 |
| 13,000 | unranked | 0.9402 | 2.35 | 425 |
| 13,000 | idf top 6 / 10 / 16 | 0.9375 / 0.9389 / 0.9393 | 0.80 / 1.04 / 1.52 | 1,254 / 965 / 660 |

So pruning is a dial, not a free speed-up: the accuracy lost at a fixed top-N GROWS with the data (more groups compete for each key). Tentative default: cap 30, idf, top 10, **calibrated per dataset**: the smallest N whose F1 is within a tolerance
of the unranked run on a labelled sample at production-like size (`gate_p.py --max-candidates N`, or `sweep3.py`, give the measurement).

**The kernel on a database** (`experiments/gate_p/`, SQLite 3.45 on ONE core, no network, so a proxy for the SHAPE only). 30,000 arrivals (6,000 for naive), the generated benchmark data, updates measured separately:

| mode | records per second | statements per record | time inside the database | rate by window (records/s) | updates per second |
|---|---|---|---|---|---|
| naive: nothing remembered, a transaction per arrival | 194 | 10.0 | 54% | 452, 325, 253, 215, 184, 179, 163, 149, 152, 134 | 121 (13.2 statements each) |
| cache1: hot cache, a transaction per arrival | 531 | 8.4 | 26% | flat, 489 to 566 | 543 (8.6) |
| **hot: cache + 1,000 arrivals per flush** | **741** | **0.78** | **10%** | 843, 788, 740, 736, 689, 769, 697, 743, 724, 702 | 570 (0.5) |
| hot, cache a SEVENTH of the data | 546 | 1.90 | 11% | 834, 716, 604, 548, 520, 500, 507, 490, 472, 457 | |

* On a 3,000-record run (`--verify`) all three modes produced groups AND an event stream identical to the in-memory kernel's; the test suite proves the same for tiny caches, every batch size, hubs, anchored updates, relationships and group merging.
* **The first hot run is flattering and I nearly reported it as the result**: at 30,000 records everything fits in the cache (0 group reads), which no real run can do. With a cache a seventh of the data the rate does not just drop, it keeps FALLING across the run as the hit rate falls (72% of group lookups,
  116,000 reads). SQLite reads are nearly free; on a networked PostgreSQL each is a round trip, so the effect there will be LARGER. **The run on the owner's PostgreSQL must include one with a cache smaller than the data.**
* Batching cut the statements per record from 10.0 to 0.78 and the database's share of the time from 54% to 10%. Whether it does on PostgreSQL is exactly what Gate P is for.
* One worker on SQLite projects to 4 workers for 2,315 records/s. That is NOT a PostgreSQL number and NOT a statement about concurrent writers.

## 7b. What Reltio and Ataccama add (read from their own documentation, October 2026)

**Reltio.** One pipeline for every route in: cleanse, generate match tokens, match, merge, "all in real time anytime a profile is created or updated"; the bulk job and the real-time API are front doors to it.
Internally it is asynchronous: a CRUD queue regenerates the tokens and sends events to a match queue, and the results flow back to update the index; create, change and merge events stream out. Processing is explicitly
**not first-in first-out**: no ordering guarantee, eventual consistency. Bulk scale comes from concurrent bulk jobs over chunks of a file. The same match technology is reused for file records against a tenant and
between tenants. The golden record is a view computed at read time; entities are graph nodes and relationships match and merge. Rules are pairwise (automatic and suspect), with a machine-learning option.

**Ataccama ONE MDM.** "The matching process operates consistently regardless of how data enters": full, initial and delta loads, streaming and APIs all go to one consolidation process. Parallelism comes from
CONFIGURATION: a few **partitions** (categories that are never matched together, with no cross-partition evaluation), **key rules** that form small clusters inside a partition, and **matching rules** applied inside a
cluster, "each of which can be computed in parallel"; the links form a graph and groups get a master ID from rule quality and order, not from a score. Proposal rules are evaluated the same way but that phase is
**serialized**. Its stated principles: **identity stability** (a master ID is not changed because the data changed: a surname change keeps it); **merge protection** (matching never automatically merges two existing
identities; a record that matches several groups goes to the best and the rest become proposals); **per-column change policies** ("rematch if changed" re-evaluates only that record and leaves the group's identity alone,
for technical columns; "create proposal if changed" raises a proposal without changing the master ID, for business-meaningful ones, and raises none for a would-be split); rematch strategies that **preserve manual
decisions**; inactive records **keep their master ID** so a returning customer gets it back; machine-learning matching only proposes, it never merges.

**What we take, and what is decided differently (items 4 and 5 are now DECIDED by the owner):**
1. **One engine, many front doors**: confirmed by Reltio, Ataccama and Senzing. Bulk, delta, streaming and reprocess are feeds of the same operation, not separate pipelines.
2. **Declared partitions** (for example person against organisation, or per tenant and domain): records in different partitions are never compared. Free parallelism and smaller candidate sets. Our domain is one level already.
3. **Parallel scoring plus a serialized assignment and review phase** (Ataccama): this is variant (b) of Gate S, a scheduling mode of the same code.
4. **DECIDED: the update policy follows Senzing and Reltio**: match a changed record again, no special identity guard (section 3). `anchored` stays available as a setting. The scorer carries a rule of our own so that a
   changed surname does not split a person (`CorroboratedScorer`, measured: 38 of 72 splits down to 8, precision unchanged).
5. **DECIDED: two existing groups ARE merged automatically, as Senzing and Reltio do** (`merge_groups="auto"`, the default): when a record joins a group and another group it matched has a golden record that matches this
   group's golden, the two merge and the older group's id survives. Measured: +5 points of recall on the multi-identifier case. Ataccama's rule is available as `merge_groups="propose"` (a "these groups may be the same" review,
   nothing merged, once per pair) and `"off"` does nothing. Downstream systems that hold a group id must therefore be told when two ids become one: the `groups_merged` event in the outbox is that notification.
6. **Per-column change policy** (rematch, propose, keep), Ataccama's model, on top of 4.
7. **Rematch and reprocess with human decisions preserved** (`PRESERVE_MANUAL` semantics: our confirm, reject and unmerge decisions are already constraints) as another feed of the same engine, for when rules change.
8. **A soft-deleted or inactive record keeps its group id**, so a returning customer rejoins it (fits the planned soft-delete flag).

## 8. Evidence (prototype, 4 seeds of the benchmark, about 730 records + 40 updates each, mean)

| | precision | recall | F1 | missed / wrong merges per run | candidates / record | seconds |
|---|---|---|---|---|---|---|
| current engines | 0.939 | 0.902 | 0.920 | 31.5 / 19.0 | n/a | 5.8 (SQLite) |
| kernel v1 (golden + identifier alternates + group merge) | 0.997 | 0.899 | 0.945 | 32.8 / 1.0 | 188 | 23.5 |
| **kernel v2 (all five decisions)** | **0.997** | **0.901** | **0.946** | 32.0 / 1.0 | 62 | 16.7 |
| v2 without feature sets | 0.997 | 0.899 | 0.945 | 32.8 / 1.0 | 62 | 9.9 |
| v2 without the frequency rule | 0.997 | 0.901 | 0.946 | 32.0 / 1.0 | 62 | 17.2 |
| v2, key cap 30 instead of 200 | 0.997 | 0.901 | 0.946 | 32.0 / 1.0 | 11 | 3.1 |

* **Accuracy is as good as the current engines everywhere except updates**, where it is far better: 72 wrong merges (over four seeds) from a record updated into someone else become 0.
* **The five decisions did not change accuracy measurably on this benchmark.** Feature sets recovered 3 missed pairs in four seeds and cost about 70% more time before the cap; the frequency rule changed
  nothing (the benchmark's shared values also have conflicting names). They are structural (competitor parity, provenance, the base for relationships and a probabilistic scorer), covered by
  tests on constructed cases, and **their benefit on real data is unmeasured.**
* **The key cap is the large win:** capping at 30 groups cut candidates per record from 188 to 11 and time by 7.5x with identical accuracy. A cap must be calibrated per dataset (from the bucket-size distribution), not fixed.
* **Relationships:** 251 of 259 household links were right (0.969); 255 of 360 household pairs were found (0.708). A first cut: the rule needs work on recall.
* **The outbox is consistent:** replaying the events reproduces the groups and links in every run (16 of 16) and in the tests.
* Arrival order changes at most a handful of record pairs (0 to 3 in the worst seed).
* **A gap in the benchmark I closed late, and what it showed.** The original update scenarios only had records that become a DIFFERENT person. Adding the opposite (`life_events=True`: the same person with a new
  surname, or a new address and phone, email unchanged), three seeds: the current engines separate 1% of those people from their groups; the kernel with the `rematch` update rule **53% (38 of 72 pairs)**; with the
  `anchored` rule **4 of 72**, still with no wrong merges on records that really became someone else, and overall F1 0.930 -> 0.949 (precision 0.996 both; recall 0.873 -> 0.906), at the price of 34 "kept after
  change" review items in three seeds. The earlier headline gain on updates was therefore one-sided.

**Not measured:** real data; the cost of a database underneath; any parallel execution; deletes; unmerge on the kernel; the learned scorer and audits on groups; a probabilistic scorer.

## 9. Tests the kernel carries (the prototype's are in `experiments/test_kernel_v2.py`: 34 tests; the application's in `tests/test_kernel_engine.py`: 30)

Replay equals state; same data and order gives the same answer and events; an update leaves its group and the golden forgets it; a corrected typo rejoins; a group left empty is removed; the feature set
finds a spelling variant and a second phone; an identifier held by many groups does not merge a record that is only that identifier, but does merge one whose name agrees; an over-populated key is not
used; households are linked and not merged; the scorer is replaceable; under the `anchored` policy a person whose name changed but whose email did not stays and is flagged, a record that shares no identifier leaves, and one that scores below the review line leaves.

## 10. Risks and open decisions

1. **Concurrent execution of the one operation (section 7).** It decides whether the 12-hour target is reachable with this design, and it must be done WITHOUT a second engine. Gate S first.
2. **Entity magnets:** a group that pools many values attracts false merges. The feature view takes the nearest value per field; if it proves a problem, weight by support.
3. **Key and cap calibration** per tenant and domain; hub names (Smith, Garcia) need a corroboration rule beyond the cap.
4. **The scorer** is still hand-set weights. A probabilistic scorer is the largest remaining accuracy lever and fits the interface.
5. **The two competitor-reading decisions are made** (section 7b items 4 and 5): update = match again as Senzing and Reltio do, with our surname rule; group merging = automatic. Both stay settings (`update_policy`, `merge_groups`).
6. **Writes are batched; PostgreSQL and the next steps are not done (section 0).** The remaining cost on SQLite is spread (about 60% Python overhead), and the rate still falls with the data (654 to about 420 records/s over 50,000): partly explained in section 0, not solved. On PostgreSQL the same code sends about 25 statements per record as network round trips, so a cache for the reads comes next (the writes are already down to about 8 statements per record). **The batched writes have never run on PostgreSQL; Gate P and Gate S (concurrent workers) are unrun.** A long batch holds SQLite's single write lock (other writers wait up to 30 s).
7. **Not yet in the application:** relationships between groups (households), the event outbox for downstream systems, the `anchored` update setting, `MDM_ENGINE` per tenant (it is global), and
   concurrent workers. A changed record can now leave its group, so a downstream system that holds entity ids must expect an entity to LOSE a record, not only to gain one.
8. **System regrouping stays off** by decision. If it is ever wanted, `resolve(group scope)` is the hook: re-run section 3 over one group's members with human constraints applied.


## 11. Which database? (written in response to "what if the customer's database is not Postgres?")

**Two different questions share that sentence.**
1. *The customer's database as a SOURCE or TARGET* (Oracle, SQL Server, a warehouse, Salesforce, files): independent of our store. Handled by connectors and files; the competitors do it with connectors (Informatica advertises 300 to 400+),
   bulk jobs and streams. We have no connector layer yet. Not blocked by anything in this document.
2. *The customer's database as the platform's own STORE* (they will not allow Postgres): depends on the deployment model. In a SaaS we host, the store is ours and the question does not arise (Reltio, AWS Entity Resolution and
   Informatica's SaaS do not let a customer choose it). It arises only for a **self-hosted** or bring-your-own-database deployment.

**What the competitors that can be self-hosted support** (their own documentation, October 2026): Ataccama MDM: PostgreSQL (and Aurora PostgreSQL), SQL Server, Azure SQL, and Oracle "self-managed only"; other flavours of
PostgreSQL are explicitly not supported. Senzing v4: PostgreSQL (Aurora, RDS), SQL Server, Azure SQL, Oracle 19c or later, and also MySQL (only below 100M records) and SQLite (only below 1M records). Both put
**PostgreSQL first, then SQL Server, then Oracle**; neither supports an arbitrary database, and Senzing states size limits for the weaker engines. MySQL would not reach our 100M target.

**What it would cost us** (counted in the code): two full hand-written stores of about 1,200 lines each (about 124 SQL statements in the Postgres one). The Postgres-specific features are few and localized: 12 upserts
(`ON CONFLICT`), about 27 JSON(B) uses, 14 array-parameter lookups (`= ANY`), 4 partial indexes, 2 serial columns, one row lock, one row-value keyset comparison, and 8 statements of row-level-security policy. The
store contract suite has 9 behaviours (run as 17 tests, 9 of them Postgres-only and skipped without a server), which is the portability test for any new backend. The kernel adds needs of its own: version-checked
updates, multi-row batched writes, and a claimable redo queue, each of which has an equivalent in SQL Server and Oracle but must be tuned per engine (Senzing publishes a database-tuning guide for exactly this).
Tenant isolation currently also relies on Postgres row-level security; SQL Server and Oracle have different mechanisms, so each store must enforce the tenant in its own queries and treat RLS as a second layer.

**Recommendation (items 1 to 3 ADOPTED and done: see `docs/database-support.md`; the deployment-model question below is still open):**
1. **Do not promise multi-database support now.** Postgres is the product store; the customer's databases are sources and targets through connectors. Prove the kernel on Postgres first (Gate P, section 7c).
2. **If a self-hosted customer requires it, add backends in the competitors' order: SQL Server and Azure SQL, then Oracle.** Not MySQL. Each is a new store of about 1,200 lines plus tuning, so do it for a paying need.
3. **Keep the door cheap to open, now (DONE: `app/sql_dialect.py`, `tests/test_dialect.py`, `tests/test_store_contract.py`, `tests/test_sql_portability.py`):** keep every SQL statement behind the store interface; extend the contract suite to the kernel's needs (version-checked update, batch write, redo claim) so it is the single portability test;
   prefer portable constructs in the new kernel tables (a version column and `UPDATE .. WHERE version = ?`, not advisory locks); do not hand-write a THIRD and FOURTH store: generate the simple statements (upsert, paging, array
   lookup) per dialect from one definition first; and make every store enforce the tenant in its own queries.
4. **A customer's data warehouse (Snowflake, Databricks, BigQuery) is not a candidate store**: the kernel does many small transactional reads and writes, which warehouses are poor at. It is a source and a target. In-warehouse
   matching is the set-based style of Splink and Tamr, a different product mode that a stateful kernel cannot provide.
