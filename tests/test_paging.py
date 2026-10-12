"""
Cursors and page sizes. Pure functions: every way a cursor can be wrong must be a clean ValueError, never an exception from deep inside a query.

Run with:  python3 -m unittest tests.test_paging -v
"""
import base64
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import paging  # noqa: E402

QUEUE = {"s": (int, float), "i": str}
ENTITY = {"i": str}


def forge(obj) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")


class TestCursors(unittest.TestCase):
    def test_a_cursor_round_trips_exactly(self):
        for score in (0.97, 0.5999999999999999, 1.0, 0, 1e-12):
            token = paging.encode({"s": score, "i": "cand_1"})
            self.assertEqual(paging.decode(token, QUEUE), {"s": score, "i": "cand_1"})
        self.assertEqual(paging.decode(paging.encode({"i": "ent_é☃"}), ENTITY), {"i": "ent_é☃"})

    def test_a_cursor_is_urlsafe_and_short(self):
        token = paging.encode({"s": 0.97, "i": "cand_" + "f" * 32})
        self.assertRegex(token, r"^[A-Za-z0-9_-]+$")
        self.assertLess(len(token), 120)

    def test_everything_that_is_not_a_cursor_for_this_list_is_refused(self):
        wrong = {
            "empty": "", "none": None, "number": 5, "not base64": "!!!notbase64!!!", "not json": base64.urlsafe_b64encode(b"hello").decode(),
            "a list": forge([1, 2]), "no version": forge({"s": 0.9, "i": "x"}), "future version": forge({"v": 2, "s": 0.9, "i": "x"}),
            "missing field": forge({"v": 1, "s": 0.9}), "extra field": forge({"v": 1, "s": 0.9, "i": "x", "tenant": "other"}),
            "score is text": forge({"v": 1, "s": "0.9", "i": "x"}), "id is a number": forge({"v": 1, "s": 0.9, "i": 7}), "score is a bool": forge({"v": 1, "s": True, "i": "x"}),
            "too long": "A" * 600, "binary": base64.urlsafe_b64encode(bytes(range(256))).decode(),
        }
        for label, token in wrong.items():
            with self.assertRaises(ValueError, msg=label) as raised:
                paging.decode(token, QUEUE)
            self.assertEqual(str(raised.exception), "invalid cursor", label)

    def test_a_cursor_for_one_list_is_not_accepted_by_the_other(self):
        with self.assertRaises(ValueError):
            paging.decode(paging.encode({"i": "ent_1"}), QUEUE)
        with self.assertRaises(ValueError):
            paging.decode(paging.encode({"s": 0.9, "i": "cand_1"}), ENTITY)


class TestPageSize(unittest.TestCase):
    def test_it_is_clamped_and_falls_back_to_the_default(self):
        self.assertEqual(paging.clamp_limit(None, 50, 200), 50)
        self.assertEqual(paging.clamp_limit("abc", 50, 200), 50)
        self.assertEqual(paging.clamp_limit(25, 50, 200), 25)
        self.assertEqual(paging.clamp_limit("25", 50, 200), 25)
        self.assertEqual(paging.clamp_limit(0, 50, 200), 1)
        self.assertEqual(paging.clamp_limit(-7, 50, 200), 1)
        self.assertEqual(paging.clamp_limit(10 ** 9, 50, 200), 200)


if __name__ == "__main__":
    unittest.main()
