# Experiment: matching a record against group golden records

**The application now runs the design measured here** (`app/kernel.py`, `app/kernel_router.py`; docs/kernel-design.md section 0). This folder keeps the prototype, the benchmark and the gates; the prototype's scorers are imported from `app/kernel.py`, so the two cannot drift apart. It measured the design (every record is matched only against group GOLDEN records; one kernel instead of three engines) against the engines the application had, on cases the shipped benchmark does not have.

* `sql_time_by_statement.py`: times every distinct SQL statement a load runs, per record, and compares two runs (an empty database against one holding 50,000 records); it showed that the SQL time per record triples with the data and where (commit, key and lineage inserts).
* `app_engine_benchmark.py [SEED ...]`: the same benchmark on the APPLICATION's engine (`pipeline.ingest_record`, a real store), kernel against legacy. Two seeds: precision 0.994 against 0.941, F1 0.942 against 0.923, recall 0.896 against 0.906; wrong merges after an update that turns a record into someone else: 0 against 36.

* `hard_cases.py` (`build(seed, scale, life_events=True)` adds updates after a life event): the benchmark. 1,227 records, 70 later updates, ground truth per record (after the updates), scored as pairwise precision / recall / F1,
  overall and per scenario. Scenarios include one person with two emails and two phones, a household sharing a phone and address, Jr / Sr, generic shared
  values, a record that mixes two people's identifiers, and updates that turn a record into someone else or correct a typo.
* `golden_kernel.py`: the prototype. In memory, but it uses the real standardization, blocking, scorer and survivorship. `bridge=True` also merges two groups
  when their goldens match each other.
* `sweep.py SEED [SEED ...]`: current engine and both kernel variants over several seeds (SCALE env, default 0.6); writes `out/sweep.json`.
* `kernel_v2.py`: version 2 with the five decisions from the competitor review (`docs/kernel-design.md`); `test_kernel_v2.py` its property tests (`python3 -m unittest experiments.test_kernel_v2`); `sweep2.py SEED ...` runs version 1, version 2 and ablations (writes `out/sweep2.json`).
* `sweep3.py SEED ...`: candidate ranking and the key cap (accuracy, groups scored per arrival, time); `CAPS=10,30 python3 experiments/sweep3.py 1 2 3`.
* `gate_p/`: **Gate P**, the kernel on a database (SQLite here, PostgreSQL on yours): `gate_p/README.md` says how to run it; `gate_p/test_gate_p.py` is its 21 correctness tests (`python3 -m unittest experiments.gate_p.test_gate_p`).
* `run_system.py SYSTEM ORDER`: `regular` and `bulk` run the application's engines (`regular` now means whatever `MDM_ENGINE` selects: the kernel by default; set `MDM_ENGINE=legacy` for the old comparison). SYSTEM is `regular`, `bulk`, `kernel` or `kernelB` (kernel + group merge); ORDER is `r11`, `r12`, ... (random, by seed),
  `grouped` or `reversed`. Writes `out/SYSTEM__ORDER__MARGIN.json`.

```
python3 experiments/run_system.py kernelB r11
python3 experiments/run_system.py bulk r11
```

One core runs about 1,300 records in 40 to 60 seconds. The numbers are on ONE seed of a benchmark I wrote: read them as a design test, not as an accuracy claim.
