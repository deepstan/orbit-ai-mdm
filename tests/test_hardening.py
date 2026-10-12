"""
The security and limits rules, as pure functions: authentication fails closed, a person's name comes from the key and never from the request, a key limited to some
domains sees only those, and one request or key cannot ask for unbounded work.

Run with:  python3 -m unittest tests.test_hardening -v
"""
import os
import sys
import threading
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import limits  # noqa: E402
from app.auth import Principal, dev_mode, human_actor, may_see_log_entry, valid_key_label, visible_domains  # noqa: E402


def principal(label="Jane", allowed=None, role="data_steward"):
    return Principal(tenant_id="acme", role=role, key_id="key_1", label=label, allowed_domains=allowed)


class TestAuthenticationFailsClosed(unittest.TestCase):
    def test_dev_mode_is_off_unless_somebody_turns_it_on(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MDM_DEV_MODE", None)
            os.environ.pop("MDM_ENABLE_AUTH", None)
            self.assertFalse(dev_mode())                                  # nothing set: authentication is REQUIRED (it used to be open)
        for value in ("1", "true", "TRUE", "yes", " 1 "):
            with mock.patch.dict(os.environ, {"MDM_DEV_MODE": value}):
                self.assertTrue(dev_mode(), value)
        for value in ("", "0", "false", "no", "off", "maybe"):
            with mock.patch.dict(os.environ, {"MDM_DEV_MODE": value}):
                self.assertFalse(dev_mode(), value)

    def test_the_old_switch_cannot_open_the_api_by_accident(self):
        for value in ("", "false", "0", "true", "1"):                     # whatever the retired variable says, it does not enable dev mode
            with mock.patch.dict(os.environ, {"MDM_ENABLE_AUTH": value}):
                os.environ.pop("MDM_DEV_MODE", None)
                self.assertFalse(dev_mode(), value)


class TestWhoDidIt(unittest.TestCase):
    def test_a_decision_is_signed_with_the_keys_label_as_a_steward(self):
        self.assertEqual(human_actor(principal("Jane")), "steward:Jane")
        self.assertEqual(human_actor(principal("  Jane  ")), "steward:Jane")

    def test_a_label_cannot_make_a_persons_decision_look_like_the_systems(self):
        for label in ("system:auto_merge", "System:Learned_Scorer", "system:"):
            self.assertFalse(human_actor(principal(label)).lower().startswith("system:"), label)

    def test_a_key_without_a_label_falls_back_to_its_id(self):
        self.assertEqual(human_actor(principal("")), "steward:key_1")

    def test_what_a_key_may_be_called(self):
        self.assertEqual(valid_key_label("  Jane's steward key "), "Jane's steward key")
        for bad in ("", "   ", None, 7, "x" * 101, "bad\nlabel", "tab\tname", "system:auto_merge", "  SYSTEM:x", "System:"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                valid_key_label(bad)
        self.assertEqual(valid_key_label("x" * 100), "x" * 100)          # exactly at the limit is fine


class TestDomainScope(unittest.TestCase):
    def test_domains_listed_are_the_ones_the_key_may_see(self):
        names = ["customer", "product", "vendor"]
        self.assertEqual(visible_domains(principal(allowed=None), names), names)
        self.assertEqual(visible_domains(principal(allowed=["product"]), names), ["product"])
        self.assertEqual(visible_domains(principal(allowed=[]), names), [])

    def test_the_audit_log_of_a_restricted_key_holds_only_its_own_domains(self):
        everything, scoped = principal(allowed=None), principal(allowed=["customer"])
        for domain in ("customer", "product", None):
            self.assertTrue(may_see_log_entry(everything, domain))
        self.assertTrue(may_see_log_entry(scoped, "customer"))
        self.assertFalse(may_see_log_entry(scoped, "product"))
        self.assertFalse(may_see_log_entry(scoped, None))                 # tenant-wide entries (a key created, a purge) are not its business


class TestRecordLimits(unittest.TestCase):
    def test_a_normal_record_passes_and_is_returned_unchanged(self):
        record = {"first_name": "Robert", "city": "Atlanta"}
        self.assertIs(limits.check_attributes(record), record)

    def test_too_many_attributes(self):
        with mock.patch.dict(os.environ, {"MDM_MAX_RECORD_FIELDS": "5"}):
            limits.check_attributes({f"f{i}": 1 for i in range(5)})
            with self.assertRaises(ValueError) as raised:
                limits.check_attributes({f"f{i}": 1 for i in range(6)})
        self.assertIn("at most 5 attributes", str(raised.exception))

    def test_too_many_bytes_counted_in_bytes_not_characters(self):
        with mock.patch.dict(os.environ, {"MDM_MAX_RECORD_BYTES": "200"}):
            limits.check_attributes({"note": "x" * 100})
            with self.assertRaises(ValueError):
                limits.check_attributes({"note": "x" * 300})
            with self.assertRaises(ValueError):
                limits.check_attributes({"note": "é" * 90})               # 90 characters, 180 bytes in UTF-8, plus the quoting: over

    def test_it_must_be_an_object(self):
        for bad in ([], "text", 3, None):
            with self.assertRaises(ValueError):
                limits.check_attributes(bad)

    def test_nonsense_settings_fall_back_to_the_defaults_and_zero_cannot_switch_a_limit_off(self):
        for value in ("lots", "-5", "", "0"):
            with mock.patch.dict(os.environ, {"MDM_MAX_BATCH_RECORDS": value}):
                self.assertEqual(limits.max_batch_records(), 10_000, value)

    def test_the_body_size_is_judged_from_the_announced_length(self):
        with mock.patch.dict(os.environ, {"MDM_MAX_BODY_BYTES": "1000"}):
            self.assertFalse(limits.body_too_large("1000"))
            self.assertTrue(limits.body_too_large("1001"))
            for header in (None, "", "abc"):
                self.assertFalse(limits.body_too_large(header))           # not judged here: the proxy must cap what announces nothing


class TestTheRateLimiter(unittest.TestCase):
    def setUp(self):
        self.now = [1000.0]
        self.make = lambda per_minute: limits.RateLimiter(per_minute, clock=lambda: self.now[0])

    def test_a_burst_up_to_the_limit_is_allowed_then_refused_with_a_wait(self):
        rl = self.make(60)
        self.assertEqual([rl.allow("k")[0] for _ in range(60)], [True] * 60)
        allowed, wait = rl.allow("k")
        self.assertFalse(allowed)
        self.assertAlmostEqual(wait, 1.0, places=6)                       # 60 a minute is one a second

    def test_it_refills_evenly_over_the_minute(self):
        rl = self.make(60)
        for _ in range(60):
            rl.allow("k")
        self.now[0] += 5.0
        self.assertEqual([rl.allow("k")[0] for _ in range(6)], [True] * 5 + [False])   # 5 seconds, 5 requests
        self.now[0] += 600.0
        self.assertEqual(sum(rl.allow("k")[0] for _ in range(100)), 60)   # a long rest does not bank more than a minute's worth

    def test_each_key_has_its_own_bucket(self):
        rl = self.make(2)
        self.assertEqual([rl.allow("a")[0] for _ in range(3)], [True, True, False])
        self.assertTrue(rl.allow("b")[0])

    def test_zero_means_unlimited(self):
        rl = self.make(0)
        self.assertTrue(all(rl.allow("k")[0] for _ in range(10_000)))

    def test_memory_stays_bounded_when_many_keys_come_and_go(self):
        rl = self.make(60)
        for i in range(12_000):
            rl.allow(f"key{i}")
            self.now[0] += 0.5                                            # each key is seen once, then idle for good
        self.assertLess(len(rl._buckets), 11_000)

    def test_threads_cannot_get_more_than_the_bucket_holds(self):
        rl = self.make(100)
        results = []

        def hammer():
            for _ in range(50):
                results.append(rl.allow("k")[0])
        threads = [threading.Thread(target=hammer) for _ in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(sum(results), 100)                               # 400 attempts at one instant: exactly the bucket, no more


if __name__ == "__main__":
    unittest.main()
