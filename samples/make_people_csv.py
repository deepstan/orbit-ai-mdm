"""
Writes N synthetic people as CSV to stdout, using the same generator as the project's benchmarks. The person domain uses the scalable blocking keys, so
the file loads in time roughly proportional to its size (the customer domain's keys are dense and do not scale like this).

    python samples/make_people_csv.py 50000 > people_50000.csv
"""
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from app import benchmark_person as bp  # noqa: E402

n = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
FIELDS = ["first_name", "last_name", "dob", "ssn", "phone", "email", "address_line1", "postal_code"]
out = csv.writer(sys.stdout, lineterminator="\n")
out.writerow(["PersonID"] + FIELDS)
for i in range(n):
    raw = bp.record_at(7, i, 0.08, **bp.DEFAULT_RATES)[0]
    out.writerow([f"P-{i:07d}"] + [raw.get(f) or "" for f in FIELDS])
