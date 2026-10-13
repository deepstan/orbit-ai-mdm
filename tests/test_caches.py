"""
The similarity and embedding caches (app/perf_cache.py) may change SPEED and nothing else.

  * every cached answer is bit for bit the uncached function's answer, for both argument orders, on empty, unicode and long strings;
  * Jaro-Winkler really is symmetric (that is what lets (a, b) and (b, a) share an entry): if its implementation ever stops being, this fails;
  * the caches are bounded, can be switched off, fail loudly on a bad setting, and never hand out a dict that a caller can corrupt;
  * the SCORER gives the same score, reasons and contributions with the caches on and off, on real benchmark pairs.

Run with:  python3 -m unittest tests.test_caches -v
"""
import os
import random
import string
import sys
import threading
import unittest
import unittest.mock as mock

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "experiments"))

from app import embeddings, matching, perf_cache  # noqa: E402
from app.domains import get_domain_config  # noqa: E402
from app.embeddings import NgramHashEmbedding, _embed_impl  # noqa: E402
from app.matching import cache_size, jaro_winkler, score_pair, similarity  # noqa: E402
from app.staging import stage_record  # noqa: E402

ALPHABET = string.ascii_lowercase + " 0123456789-'.éüñøßあ"


def random_strings(n, seed, longest=30):
    rng = random.Random(seed)
    return ["".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, longest))) for _ in range(n)]


class CacheCase(unittest.TestCase):
    def setUp(self):
        self._saved = (matching._jw, embeddings._embed_cached)
        self.addCleanup(self._restore)

    def _restore(self):
        matching._jw, embeddings._embed_cached = self._saved


class TestSimilarityIsUnchanged(CacheCase):
    def test_every_cached_answer_is_the_uncached_answer_for_both_orders(self):
        perf_cache.configure(similarity=4096)
        words = random_strings(400, 1) + ["", "a", "é" * 40, "x" * 200, "jennifer", "jenifer", "maría josé", "maria jose"]
        rng = random.Random(2)
        for _ in range(50_000):
            a, b = rng.choice(words), rng.choice(words)
            want = 0.0 if not a or not b else jaro_winkler(a, b)
            self.assertEqual(similarity(a, b), want, (a, b))
            self.assertEqual(similarity(b, a), 0.0 if not a or not b else jaro_winkler(b, a), (b, a))

    def test_jaro_winkler_is_symmetric_bit_for_bit_which_is_what_lets_two_orders_share_an_entry(self):
        words = random_strings(600, 3, longest=25)
        rng = random.Random(4)
        for _ in range(50_000):
            a, b = rng.choice(words), rng.choice(words)
            if a and b:
                self.assertEqual(jaro_winkler(a, b), jaro_winkler(b, a), (a, b))

    def test_the_second_call_is_a_hit_and_so_is_the_reverse_order(self):
        perf_cache.configure(similarity=100)
        similarity("katherine", "catherine")
        info = matching._jw.cache_info()
        self.assertEqual((info.hits, info.misses), (0, 1))
        similarity("katherine", "catherine")
        similarity("catherine", "katherine")
        info = matching._jw.cache_info()
        self.assertEqual((info.hits, info.misses, info.currsize), (2, 1, 1))

    def test_empty_is_no_signal_and_is_never_cached(self):
        perf_cache.configure(similarity=100)
        self.assertEqual((similarity("", "abc"), similarity("abc", ""), similarity("", "")), (0.0, 0.0, 0.0))
        self.assertEqual(matching._jw.cache_info().currsize, 0)


class TestTheCachesAreBoundedAndConfigurable(CacheCase):
    def test_the_cache_never_holds_more_than_its_size(self):
        perf_cache.configure(similarity=100, embedding=50)
        for a, b in zip(random_strings(1500, 5, 12), random_strings(1500, 6, 12)):
            similarity(a, b)
        for t in random_strings(400, 7, 20):
            embeddings._embed_cached(t, 3, 512)
        self.assertLessEqual(matching._jw.cache_info().currsize, 100)
        self.assertLessEqual(embeddings._embed_cached.cache_info().currsize, 50)
        self.assertEqual(perf_cache.stats()["similarity"]["max_size"], 100)

    def test_size_zero_turns_a_cache_off_and_the_answers_do_not_change(self):
        words = random_strings(60, 8)
        on = [similarity(a, b) for a in words for b in words]
        perf_cache.configure(similarity=0, embedding=0)
        off = [similarity(a, b) for a in words for b in words]
        self.assertEqual(on, off)
        self.assertEqual(matching._jw.cache_info().currsize, 0)
        e = NgramHashEmbedding()
        self.assertEqual(e.similarity("Bob's Hardware Inc", "Robert's Hardware Company"), e.similarity("Bob's Hardware Inc", "Robert's Hardware Company"))
        self.assertEqual(embeddings._embed_cached.cache_info().currsize, 0)

    def test_the_size_comes_from_the_environment_and_a_bad_value_fails_loudly(self):
        with mock.patch.dict(os.environ, {"MDM_X": "2500"}):
            self.assertEqual(cache_size("MDM_X", 7), 2500)
        with mock.patch.dict(os.environ, {"MDM_X": "0"}):
            self.assertEqual(cache_size("MDM_X", 7), 0)
        for unset in ("", "  "):
            with mock.patch.dict(os.environ, {"MDM_X": unset}):
                self.assertEqual(cache_size("MDM_X", 7), 7)
        self.assertEqual(cache_size("MDM_NO_SUCH_SETTING_AT_ALL", 7), 7)
        for bad in ("lots", "1.5", "-3"):
            with mock.patch.dict(os.environ, {"MDM_X": bad}), self.assertRaises(ValueError) as cm:
                cache_size("MDM_X", 7)
            self.assertIn("MDM_X", str(cm.exception))

    def test_stats_report_hits_and_clear_empties_both(self):
        perf_cache.configure(similarity=100, embedding=100)
        for _ in range(3):
            similarity("anne", "ann")
            NgramHashEmbedding().similarity("12 oak st", "12 oak street")
        s = perf_cache.stats()
        self.assertEqual((s["similarity"]["hits"], s["similarity"]["misses"]), (2, 1))
        self.assertAlmostEqual(s["similarity"]["hit_rate"], 2 / 3)
        self.assertGreater(s["embedding"]["hits"], 0)
        perf_cache.clear()
        self.assertEqual((matching._jw.cache_info().currsize, embeddings._embed_cached.cache_info().currsize), (0, 0))
        self.assertIsNone(perf_cache.stats()["similarity"]["hit_rate"])


class TestTheEmbeddingIsUnchangedAndSafe(CacheCase):
    def test_the_cached_vector_is_the_vector_the_function_computes(self):
        e = NgramHashEmbedding()
        for text in random_strings(300, 9, 40) + ["", "a", "Bob's Hardware Inc.", "Robert's Hardware Company", "ñandú"]:
            self.assertEqual(e.embed(text), _embed_impl(text, e.n, e.buckets), text)
            self.assertEqual(e.embed(text), _embed_impl(text, e.n, e.buckets), text)              # and the second, cached, call

    def test_a_caller_that_changes_the_vector_it_was_given_corrupts_nothing(self):
        e = NgramHashEmbedding()
        first = e.embed("12 oak street")
        want = dict(first)
        first.clear()
        first[999] = 42.0
        self.assertEqual(e.embed("12 oak street"), want)
        self.assertAlmostEqual(e.similarity("12 oak street", "12 oak street"), 1.0, places=12)       # identical text still scores 1 after a caller wrecked its copy

    def test_the_settings_of_the_embedding_are_part_of_the_key(self):
        a, b = NgramHashEmbedding(n=3, buckets=512), NgramHashEmbedding(n=2, buckets=64)
        self.assertNotEqual(a.embed("main street"), b.embed("main street"))
        self.assertEqual(a.embed("main street"), _embed_impl("main street", 3, 512))
        self.assertEqual(b.embed("main street"), _embed_impl("main street", 2, 64))

    def test_similarity_is_what_it_was_for_default_and_non_default_settings(self):
        defaults = set()
        for e in (NgramHashEmbedding(), NgramHashEmbedding(n=2, buckets=64), NgramHashEmbedding(n=4, buckets=2048)):
            for a, b in (("Bob's Hardware Inc", "Robert's Hardware Company"), ("12 oak street", "12 oak st"), ("", "x"), ("x", ""), ("same", "same")):
                va, vb = _embed_impl(a, e.n, e.buckets), _embed_impl(b, e.n, e.buckets)
                smaller, larger = (va, vb) if len(va) < len(vb) else (vb, va)
                want = 0.0 if not a or not b else max(0.0, min(1.0, sum(v * larger.get(k, 0.0) for k, v in smaller.items())))
                self.assertEqual(e.similarity(a, b), want, (e.n, e.buckets, a, b))
                if a == "12 oak street":
                    defaults.add(want)
        self.assertEqual(len(defaults), 3, "the three settings must give three different answers, or this test cannot tell them apart")


class TestTheScorerIsUnchanged(CacheCase):
    def test_scores_reasons_and_contributions_are_identical_with_the_caches_on_and_off(self):
        import hard_cases as hc
        cfg = get_domain_config("customer")
        data = hc.build(2, 0.25)
        staged = [stage_record("customer", cfg, r["raw"])[0] for r in data["rows"]]
        rng = random.Random(10)
        pairs = [(rng.choice(staged), rng.choice(staged)) for _ in range(1500)] + [(staged[i], staged[i + 1]) for i in range(0, len(staged) - 1, 2)]

        def run():
            return [(sb.score, sb.match_type, tuple(sb.reasons), repr(sb.contributions)) for sb in (score_pair(a, b, cfg) for a, b in pairs)]

        perf_cache.configure(similarity=131_072, embedding=16_384)
        on, on_again = run(), run()
        perf_cache.configure(similarity=0, embedding=0)
        off = run()
        self.assertEqual(on, off)
        self.assertEqual(on, on_again)
        self.assertGreater(len({score for score, *_ in on}), 20, "the comparison must not be vacuous: the pairs have to produce different scores")


class TestThreads(CacheCase):
    def test_many_threads_calling_at_once_get_the_right_answers(self):
        perf_cache.configure(similarity=500, embedding=100)
        words = random_strings(200, 11, 15)
        errors = []

        def work(seed):
            rng = random.Random(seed)
            for _ in range(4000):
                a, b = rng.choice(words), rng.choice(words)
                want = 0.0 if not a or not b else jaro_winkler(a, b)
                if similarity(a, b) != want:
                    errors.append((a, b))

        threads = [threading.Thread(target=work, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
