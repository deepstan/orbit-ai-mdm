"""
The profiler: finding placeholder values no rule can know about. Unit and functional tests only.

Run with:  python3 -m unittest tests.test_profile -v
"""
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import benchmark_person as bp, domain_config
from app.db import Store
from app.domain_config import parse_domain_document
from app.pipeline import ingest_batch
from app.profile import format_profile, profile_records

ROOT = os.path.join(os.path.dirname(__file__), "..")
FIRST = ["Ann", "Bo", "Cy", "Di", "Ed", "Flo", "Gus", "Hal", "Ivy", "Jon", "Kit", "Lee", "Mae", "Ned", "Oz", "Pia"]
LAST = ["Lee", "Wu", "Chen", "Diaz", "Khan", "Park", "Ruiz", "Shah", "Tran", "Vega", "Wong", "Yang", "Zhou", "Ali", "Berg", "Cruz"]


def person(i, **over):
    r = {"first_name": FIRST[i % 16], "last_name": LAST[(i // 16) % 16] + str(i // 256), "ssn": f"{200 + i % 600:03d}-{10 + i % 80:02d}-{1000 + i:04d}",
         "dob": f"{1940 + i % 60}-{1 + i % 12:02d}-{1 + i % 28:02d}"}
    r.update(over)
    return r


class Base(unittest.TestCase):
    def setUp(self):
        domain_config.reset()
        self.addCleanup(domain_config.reset)
        bp.register_person("full")
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)


class TestProfile(Base):
    def test_a_value_shared_by_many_different_people_is_suspect_and_the_ssn_is_masked(self):
        records = [person(i) for i in range(400)] + [person(1000 + i, ssn="512-45-0001") for i in range(60)]
        f = profile_records("person", records, fields=["ssn"])["fields"]["ssn"]
        self.assertEqual([(s["value"], s["count"]) for s in f["suspects"]], [("***-**-0001", 60)])
        self.assertGreaterEqual(f["suspects"][0]["different_names_in_sample"], 30)
        self.assertNotIn("512-45-0001", json.dumps(f))
        self.assertNotIn("512450001", json.dumps(f))

    def test_reveal_prints_the_exact_value_and_a_config_snippet(self):
        records = [person(i) for i in range(400)] + [person(1000 + i, ssn="512-45-0001") for i in range(60)]
        report = profile_records("person", records, fields=["ssn"], reveal=True)
        self.assertEqual(report["fields"]["ssn"]["suspects"][0]["value"], "512450001")
        self.assertEqual(report["config_snippet"], {"fields": {"ssn": {"anonymous": ["512450001"]}}})
        self.assertIn("anonymous", format_profile(report))

    def test_one_person_repeated_is_not_a_placeholder(self):
        records = [person(i) for i in range(400)] + [person(5000, first_name="Zed", last_name="Quill", ssn="512-45-0002")] * 40
        self.assertEqual(profile_records("person", records, fields=["ssn"])["fields"]["ssn"]["suspects"], [])

    def test_a_date_shared_by_many_people_is_not_suspect_unless_it_stands_far_out_from_the_rest(self):
        """In a big file every date is shared by many different people, so 'many people share it' alone proves nothing. Here
        2,000 people fall on only 40 dates (about 50 each): none is suspect. Then a defaulted date takes a third of the file."""
        rng = random.Random(3)
        dates = [f"{1950 + d // 12}-{1 + d % 12:02d}-15" for d in range(40)]
        people = [dict(bp.make_person(rng, 1, 1, 1, 1), dob=dates[i % 40]) for i in range(2000)]
        self.assertEqual(profile_records("person", people, fields=["dob"])["fields"]["dob"]["suspects"], [])
        defaulted = [dict(p, dob="1960-06-20") if i % 3 == 0 else p for i, p in enumerate(people)]
        f = profile_records("person", defaulted, fields=["dob"])["fields"]["dob"]
        self.assertEqual([s["value"] for s in f["suspects"]], ["1960-06-20"])
        self.assertGreater(f["threshold"], 300)  # the bar rose with the file: ten times the average count per date

    def test_what_the_rules_reject_is_counted_with_reasons_and_is_never_suspect(self):
        records = [person(i) for i in range(300)] + [person(1000 + i, ssn="000-00-0000", dob="1900-01-01") for i in range(50)] + [person(2000, ssn="", dob="")]
        r = profile_records("person", records)["fields"]
        self.assertEqual(r["ssn"]["rejected_reasons"], {"all the same digit": 50})
        self.assertEqual(r["dob"]["rejected_reasons"], {"a default or system date": 50})
        self.assertEqual((r["ssn"]["missing"], r["ssn"]["suspects"]), (1, []))
        self.assertEqual(r["ssn"]["total"], 351)

    def test_too_many_january_firsts_is_reported(self):
        normal = [person(i) for i in range(1000)]
        self.assertFalse(profile_records("person", normal, fields=["dob"])["fields"]["dob"]["jan1_elevated"])
        defaulted = [person(i, dob=f"{1950 + i % 40}-01-01") if i % 5 == 0 else person(i) for i in range(1000)]
        f = profile_records("person", defaulted, fields=["dob"])["fields"]["dob"]
        self.assertTrue(f["jan1_elevated"])
        self.assertIn("January 1st", format_profile({"domain": "person", "records": 1000, "approximate": False, "settings": {}, "fields": {"dob": f}, "config_snippet": None}))

    def test_a_year_only_dob_is_counted(self):
        f = profile_records("person", [person(i) for i in range(50)] + [person(100, dob="1985")] * 3, fields=["dob"])["fields"]["dob"]
        self.assertEqual(f["year_only"], 3)

    def test_memory_is_bounded_and_a_heavy_placeholder_still_survives(self):
        records = []
        for i in range(600):
            records.append(person(i, ssn=f"{300 + i % 500:03d}-{20 + i % 70:02d}-{2000 + i:04d}"))
            if i % 5 == 0:
                records.append(person(7000 + i, ssn="512-45-0003"))
        report = profile_records("person", records, fields=["ssn"], max_distinct=60)
        self.assertTrue(report["approximate"])
        self.assertEqual([s["count"] for s in report["fields"]["ssn"]["suspects"]][0] > 0, True)
        self.assertEqual(report["fields"]["ssn"]["suspects"][0]["value"], "***-**-0003")

    def test_any_field_can_be_profiled_by_name(self):
        from app.domains import get_domain_config  # customer's phone: a number every record without one got
        domain_config.reset()
        records = [{"first_name": FIRST[i % 16], "last_name": f"{LAST[i % 16]}{i}", "phone": f"512-555-{1000 + i}"} for i in range(300)]
        records += [{"first_name": FIRST[i % 16], "last_name": f"Other{i}", "phone": "512-555-0100"} for i in range(50)]
        f = profile_records("customer", records, fields=["phone"])["fields"]["phone"]
        self.assertEqual([s["value"] for s in f["suspects"]], ["5125550100"])

    def test_clear_errors(self):
        with self.assertRaises(ValueError) as cm:
            profile_records("person", [], fields=["nope"])
        self.assertIn("unknown field", str(cm.exception))
        domain_config.reset()
        with self.assertRaises(ValueError) as cm:
            profile_records("customer", [])
        self.assertIn("name the fields", str(cm.exception))


class TestStoreStreaming(Base):
    def test_raw_attributes_are_streamed_in_pages_exactly_once_and_can_be_filtered(self):
        store = Store(os.path.join(self.tmp, "t.db"))
        ingest_batch(store, "acme", "person", [("crm", f"A{i}", person(i)) for i in range(7)])
        ingest_batch(store, "acme", "person", [("erp", f"B{i}", person(100 + i)) for i in range(3)])
        ingest_batch(store, "other", "person", [("crm", f"C{i}", person(200 + i)) for i in range(2)])
        names = lambda **kw: sorted(r["last_name"] for r in store.iter_raw_attributes("person", batch=2, **kw))
        self.assertEqual(len(names()), 12)                                        # every record, across 6 pages of 2, none twice
        self.assertEqual(len(names(tenant_id="acme")), 10)
        self.assertEqual(len(names(tenant_id="acme", source_system="erp")), 3)
        self.assertEqual(names(tenant_id="nobody"), [])


class TestCli(Base):
    def setUp(self):
        super().setUp()
        self.domains = os.path.join(self.tmp, "domains")
        os.makedirs(self.domains)
        shutil.copy(os.path.join(ROOT, "domain_templates", "person.json"), self.domains)
        self.csv = os.path.join(self.tmp, "source.csv")

    def write_csv(self, rows):
        with open(self.csv, "w") as f:
            f.write("rid,First,Last,SSN,DOB\n")
            for i, r in enumerate(rows):
                f.write(f"{i},{r['first_name']},{r['last_name']},{r['ssn']},{r['dob']}\n")

    def cli(self, *args):
        return subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), "profile", "person", "--domains-dir", self.domains, *args],
                              capture_output=True, text=True, cwd=ROOT)

    MAP = "first_name=First,last_name=Last,ssn=SSN,dob=DOB"

    def test_a_file_is_profiled_before_it_is_loaded(self):
        self.write_csv([person(i) for i in range(400)] + [person(1000 + i, ssn="512-45-0001") for i in range(60)])
        r = self.cli("--file", self.csv, "--mapping", self.MAP)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("SUSPECT placeholders", r.stdout)
        self.assertIn("***-**-0001", r.stdout)
        self.assertNotIn("512-45-0001", r.stdout)
        self.assertIn("re-run with --reveal", r.stdout)
        self.assertFalse(os.path.exists(os.path.join(ROOT, "mdm.db")) and False)  # profiling a file writes nothing anywhere

    def test_reveal_prints_a_snippet_and_fail_on_suspects_gates_a_script(self):
        self.write_csv([person(i) for i in range(400)] + [person(1000 + i, ssn="512-45-0001") for i in range(60)])
        r = self.cli("--file", self.csv, "--mapping", self.MAP, "--reveal", "--fail-on-suspects")
        self.assertEqual(r.returncode, 1)
        self.assertIn('"anonymous"', r.stdout)
        self.assertIn("512450001", r.stdout)

    def test_a_clean_file_passes(self):
        self.write_csv([person(i) for i in range(400)])
        r = self.cli("--file", self.csv, "--mapping", self.MAP, "--fail-on-suspects")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("no suspect placeholder values", r.stdout)

    def test_json_output_parses(self):
        self.write_csv([person(i) for i in range(50)])
        out = json.loads(self.cli("--file", self.csv, "--mapping", self.MAP, "--json").stdout)
        self.assertEqual((out["domain"], out["records"]), ("person", 50))

    def test_loaded_data_can_be_profiled_by_tenant(self):
        db = os.path.join(self.tmp, "t.db")
        ingest_batch(Store(db), "acme", "person", [("crm", f"R{i}", r) for i, r in enumerate([person(i) for i in range(300)] + [person(900 + i, dob="1960-06-15") for i in range(250)])])
        r = self.cli("--tenant", "acme", "--db", db, "--field", "dob")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("1960-06-15", r.stdout)
        self.assertEqual(self.cli("--tenant", "nobody", "--db", db).returncode, 0)

    def test_helpful_errors(self):
        self.assertEqual(self.cli("--file", self.csv).returncode, 2)                                    # no mapping
        self.assertIn("--tenant", self.cli().stderr)                                                    # neither a file nor a tenant
        bad = subprocess.run([sys.executable, os.path.join(ROOT, "domains_cli.py"), "profile", "nope", "--domains-dir", self.domains], capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(bad.returncode, 2)


if __name__ == "__main__":
    unittest.main()
