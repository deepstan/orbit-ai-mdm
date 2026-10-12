"""
Unmerge, the audit page's logic and the bulk engines' separations are not specific to customers: the same lifecycle on the other built-in domains.
Each domain gets a chain A-B-C (A-B share one identifier, B-C another, A and C share nothing) built to respect that domain's EXCLUSIVE fields (a product
with a different UPC is a different product, so A simply has no UPC).

Run with:  python3 -m unittest tests.test_separations_domains -v
"""
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.bulk import bulk_load  # noqa: E402
from app.db import Store  # noqa: E402
from app.pipeline import ingest_record  # noqa: E402
from app.stewardship import audit_queue, give_verdict  # noqa: E402
from app.unmerge import separation_violations  # noqa: E402

T = "t"
P = {"product_name": "Trail Boot", "brand": "Ridge", "category": "Footwear"}
V = {"company_name": "Acme Supply", "city": "Atlanta"}
DOMAINS = {
    "product": dict(A={**P, "sku": "SKU-100"}, B={**P, "sku": "SKU-100", "upc": "036000291452"}, C={**P, "sku": "SKU-300", "upc": "036000291452"},
                    D={**P, "sku": "SKU-100", "upc": "036000291452"}, A2={**P, "sku": "SKU-100", "product_name": "Trail Boot Mk2"}),
    "vendor": dict(A={**V, "tax_id": "12-3456789"}, B={**V, "tax_id": "12-3456789", "contact_email": "ap@acme.example"}, C={**V, "contact_email": "ap@acme.example"},
                   D={**V, "tax_id": "12-3456789", "contact_email": "ap@acme.example"}, A2={**V, "tax_id": "12-3456789", "company_name": "Acme Supply Co"}),
}


class TestOtherDomains(unittest.TestCase):
    def test_the_whole_lifecycle_works_in_every_domain(self):
        for domain, r in DOMAINS.items():
            with self.subTest(domain=domain):
                tmp = tempfile.mkdtemp()
                self.addCleanup(shutil.rmtree, tmp, True)
                with mock.patch.dict(os.environ, {"MDM_AUDIT_RATE": "1", "MDM_AUDIT_RATE_PROMOTED": "1"}):
                    store = Store(os.path.join(tmp, "t.db"))
                    for sid in "ABC":
                        ingest_record(store, T, domain, "src", sid, r[sid])
                    recs = {x.source_record_id: x for x in store.list_source_records(T, domain)}
                    ent = lambda n: store.get_source_record(recs[n].id, T, domain).entity_id
                    pairs = store.confirmed_pairs_among(T, domain, [x.id for x in recs.values()])
                    self.assertEqual((len({ent(n) for n in "ABC"}), len(pairs)), (1, 2), "the chain did not form: the test data no longer fits the domain's rules")
                    ab = next(c for c, a, b in pairs if {a, b} == {recs["A"].id, recs["B"].id})
                    item = next(i for i in audit_queue(store, T, domain)["items"] if i["candidate_id"] == ab)
                    self.assertEqual(item["unmerge"], {"possible": True, "moves": 2, "stays": 1})
                    self.assertTrue(give_verdict(store, T, domain, ab, "wrong", "jane", unmerge=True)["unmerge"]["ok"])
                    self.assertNotEqual(ent("A"), ent("B"))
                    self.assertEqual(ent("B"), ent("C"))
                    # a record matching both sides, onboarded through the bulk engine: held, and nothing is reunited
                    res = bulk_load(store, T, domain, [("src2", "D1", r["D"])], onboard=True)
                    self.assertGreaterEqual(res.separations_held, 1)
                    self.assertEqual(separation_violations(store, T, domain), [])
                    # a changed record meeting the record a person separated from it: not proposed again
                    res = bulk_load(store, T, domain, [("src", "A", r["A2"])], update=True)
                    self.assertEqual((res.changed_records, res.separations_dropped), (1, 1))
                    self.assertNotEqual(ent("A"), ent("B"))
                    self.assertEqual(separation_violations(store, T, domain), [])


if __name__ == "__main__":
    unittest.main()
