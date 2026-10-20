"""Where does the SQL time go? Times every distinct SQL statement a load runs (SQLite), per record, so two runs can be compared: for example a load onto an EMPTY database against the same
load onto a database that already holds 50,000 records, to see which statements get slower as the data grows.

    python3 experiments/sql_time_by_statement.py --out small.json --n 1500 -- --tenant t --domain person --file people_1500.csv --mapping "..." --id-column PersonID --db small.db
    python3 experiments/sql_time_by_statement.py --out big.json   --n 2000 -- --tenant t --domain person --file next_2000.csv   --mapping "..." --id-column PersonID --db big.db
    python3 experiments/sql_time_by_statement.py --compare small.json big.json

Everything after `--` is passed to bulk_ingest.py unchanged (--n is how many records that load has). What it found on 2026-10-15 is in docs/kernel-design.md section 0.
"""
import atexit
import json
import os
import re
import runpy
import sqlite3
import sys
import time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def compare(a_path, b_path, top=10):
    a, b = json.load(open(a_path)), json.load(open(b_path))
    rows = []
    for k in set(a["stmts"]) | set(b["stmts"]):
        ca, ta = a["stmts"].get(k, [0, 0.0])
        cb, tb = b["stmts"].get(k, [0, 0.0])
        rows.append(((tb / b["n"] - ta / a["n"]) * 1000, k, ta / a["n"] * 1000, tb / b["n"] * 1000, cb / b["n"]))
    rows.sort(reverse=True)
    print(f"SQL time per record: {sum(v[1] for v in a['stmts'].values()) / a['n'] * 1000:.2f} ms ({a_path}) -> {sum(v[1] for v in b['stmts'].values()) / b['n'] * 1000:.2f} ms ({b_path})")
    for d, k, x, y, c in rows[:top]:
        print(f"{d:+6.2f} ms ({x:5.2f} -> {y:5.2f}), {c:4.1f}/record  {k[:110]}")


def main(argv):
    if argv and argv[0] == "--compare":
        return compare(argv[1], argv[2])
    out, n = argv[argv.index("--out") + 1], int(argv[argv.index("--n") + 1])
    rest = argv[argv.index("--") + 1:]
    timings: dict = {}

    class Timed(sqlite3.Connection):
        def execute(self, sql, *a):
            t = time.perf_counter()
            try:
                return super().execute(sql, *a)
            finally:
                d = timings.setdefault(re.sub(r"\s+", " ", sql).strip()[:120], [0, 0.0])
                d[0] += 1
                d[1] += time.perf_counter() - t

        def executemany(self, sql, *a):
            t = time.perf_counter()
            try:
                return super().executemany(sql, *a)
            finally:
                d = timings.setdefault("MANY " + re.sub(r"\s+", " ", sql).strip()[:115], [0, 0.0])
                d[0] += 1
                d[1] += time.perf_counter() - t
    real = sqlite3.connect
    sqlite3.connect = lambda *a, **k: real(*a, factory=Timed, **k)
    atexit.register(lambda: json.dump({"n": n, "stmts": timings}, open(out, "w")))
    sys.path.insert(0, ROOT)
    os.chdir(ROOT)
    sys.argv = ["bulk_ingest.py", *rest]
    try:
        runpy.run_path("bulk_ingest.py", run_name="__main__")
    except SystemExit:
        pass


if __name__ == "__main__":
    main(sys.argv[1:])
