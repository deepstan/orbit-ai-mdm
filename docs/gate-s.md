# Gate S, step 1: can many workers give nearly the answers one worker gives? (a simulation)

Status (2026-10-19): **the simulation passes the design's gate, on a benchmark, with one important redesign of how a stale read is validated, and with one risk it cannot test.**
Built: `experiments/gate_s/gate_s.py`, `experiments/gate_s/test_gate_s.py` (13 tests). **Not built:** anything in the application, and step 2 (real threads on PostgreSQL).

## What was simulated

The in-memory kernel, run by W "workers" in waves. All W of a wave READ the same state (the groups a record would look at, a signature of each, and what it would decide), then they commit in a random order; a
commit first VALIDATES its read against the state as it is by then. A read that fails validation is a CONFLICT: the record goes to a redo queue and is read again in a later wave; after `max_rounds` failed
tries it is committed alone, as if under a lock. Whatever commits runs the kernel's real `arrive` against the current state, so the final groups are a serial execution in commit order (a test replays the
commit order and requires identical groups). **This means concurrency can cost only ORDER and REDO WORK; the simulation measures both. It does not measure a database, locks or time.**

Three ways to validate a read were compared:

| Mode | A read is a conflict when | |
|---|---|---|
| `version` | the candidate set differs, or any candidate group's version changed (a row version bumped on every write) | what the design described |
| `content` | the same, but by what scoring reads (the golden and the feature sets) | same result as `version` |
| `redecide` | **the record's DECISION would change**: scores of groups that did not change are reused, groups that changed or are new are scored again, and only a different decision is a conflict | the recommendation |

## Results (benchmark data: `experiments/hard_cases.py`, one core, in memory)

Validation by **re-deciding**, the conflict rate and what it cost in accuracy against the sequential run:

| records | workers | conflicts | max tries | serialised | F1 change vs sequential | records grouped differently |
|---|---|---|---|---|---|---|
| 7,363 | 8 / 64 / 512 / 4,096 | 0.1% / 0.3% / 1.8% / 12.4% | 2 / 2 / 2 / 3 | 0 | 0 / 0 / 0 / -0.085 pts | 0 / 0 / 0 / 8 |
| 14,769 | 8 / 64 / 512 / 4,096 | 0.0% / 0.1% / 0.9% / 6.2% | 2 / 2 / 2 / 3 | 0 | 0 / -0.017 / +0.017 / +0.035 pts | 0 / 3 / 6 / 30 |
| 30,835 | 512 / 4,096 | 0.4% / 3.2% | 2 / 3 | 0 | +0.016 / +0.010 pts | 6 / 63 |

The same 7,363 records validated by a **plain group version**:

| workers | conflicts | of which FALSE ALARMS | records committed alone (after 12 retries) | F1 change |
|---|---|---|---|---|
| 8 | 2.7% | 98% | 0 | 0 |
| 64 | 19.5% | 99% | 30 | -0.017 pts |
| 512 | 61.6% | 98% | 325 | 0 |
| 4,096 | 87.8% | 98% | **2,711 (36% of all records)** | -0.144 pts |

(A false alarm: the group changed, but the record would have decided the same. Some records hit the 12-retry cap even with 8 workers.)

## What this says

1. **The gate as the design wrote it passes**: F1 within 0.5 points of sequential (the worst is -0.144 points), and the redo queue drains (never more than 13 tries, and with re-deciding never more than 3).
2. **But the validation the design described is the wrong one.** "Commit only if no group's version changed" turns 98 to 99% of its conflicts into wasted work, and at 4,096 workers it leaves about 500 of them doing anything useful.
   **Validate by re-deciding instead**: keep the scores of groups that did not change, score again only the groups that changed or appeared (2 to 6 more scorings per attempt, measured), and call it a conflict only when the decision
   itself would differ. That gave 3,965 effective workers out of 4,096 on 30,835 records.
3. **A phantom must be checked as well as a version**: two new records of one person read together find no group, so a version check on groups they read says nothing. The candidate SET is compared too (a test covers it).
4. **Conflicts fall as the data grows**: at 4,096 workers 12.4%, 6.2%, 3.2% as the file doubles. That is roughly workers divided by records. At 100 million records and 4,096 workers that would be a conflict rate near zero:
   **an extrapolation from three points on one benchmark, not a measurement.**
5. **Shuffled input is not needed here** (the design planned it). Arrival order was random already; whether sorted real files conflict more is untested.

## What it cannot tell us (read this before relying on it)

* **Hot groups.** No conflict in any run touched a group of more than 20 members: **the benchmark contains no hub entities**. Real data has them (one organisation with thousands of records, a very common name) and
  every join to one changes its group. Re-deciding helps, because joining a big group rarely changes the next record's decision, but **this is the main unproven risk**. Step 2 needs data with real hubs.
* **A database.** Row locks, version columns, round trips, commit time and the redo queue's own cost are not here. Waves are synchronous (all W read together), which is pessimistic for conflicts and says nothing about throughput.
* **The decisions that are not a join or a new group**: a review, a group merge, an ambiguity within the margin. The "same decision" test compares the group joined (or "new"); a change in those side effects is not counted.
* **Size.** 31,000 records, not the design's 1,000,000+ (the sandbox has one core). The trend is clear; the scale is not tested.
* **The validation's own cost in a real system.** Re-deciding at commit needs the changed groups read again inside the commit; that is a second read of the hot groups.

## What step 2 (real threads on PostgreSQL) should measure, if you go on

Throughput against the number of workers; the conflict rate under load **on data with hubs**; the cost of the commit-time re-read; a projection to 2,315 records per second on a stated number of cores. It needs Gate P's per-worker
number first. Design changes for the application, from this result: validate by re-deciding; compare the candidate set as well as the groups; reuse scores of unchanged groups; a small retry cap that falls back to
committing alone, as a lock on the hot group.

## Run it

```
python3 experiments/gate_s/gate_s.py --scale 12 --workers 8,64,512,4096 --modes redecide
python3 experiments/gate_s/gate_s.py --scale 6 --workers 8,64,512,4096 --modes version
python3 -m unittest experiments.gate_s.test_gate_s -v
```
(`--scale 6` is about 7,400 records; time grows with records and, for the strict modes, with the worker count; one core.)
