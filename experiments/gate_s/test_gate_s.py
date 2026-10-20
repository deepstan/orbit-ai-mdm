"""
Tests for the Gate S simulation (experiments/gate_s/gate_s.py): the MECHANISM, not the results (those are in docs/gate-s.md).

  * `decide` (the read-only decision the simulation compares) AGREES with the kernel's own `arrive`, record by record: otherwise "same decision" would mean nothing
  * what commits is a SERIAL EXECUTION: replaying the commit order one record at a time gives exactly the same groups, for every validation mode, so concurrency can cost only order and redo work
  * a PHANTOM (two new records of one person read together) is caught and resolved into one group
  * a group that changed WITHOUT changing the decision is a conflict for a strict validator and NOT for validation by re-deciding
  * one worker is the sequential run; the redo queue drains; the cap serialises a record that will not settle; runs are deterministic

Run with:  python3 -m unittest experiments.gate_s.test_gate_s -v
"""
import os
import sys
import unittest
import unittest.mock as mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("MDM_AUDIT_RATE", "0")

import gate_s as gs  # noqa: E402
import hard_cases as hc  # noqa: E402

MODES = ("version", "content", "redecide")
_DATA = {}


def data():
    if "d" not in _DATA:
        d = hc.build(3, 0.5)
        raw = {r["rid"]: r["raw"] for r in d["rows"]}
        order = [r["rid"] for r in d["rows"]]
        import random
        random.Random(11).shuffle(order)
        _DATA.update(d=d, raw=raw, order=order)
    return _DATA["d"], _DATA["raw"], _DATA["order"]


class TestTheReadOnlyDecisionMatchesTheKernel(unittest.TestCase):
    def test_before_every_arrival_decide_says_what_arrive_then_does(self):
        d, raw, order = data()
        k = gs.new_kernel()
        mismatches = []
        for rid in order:
            matching, cands, _ = gs.derive(k, raw[rid], "content")
            target, _, _ = gs.decide(k, matching, cands)
            k.arrive(rid, raw[rid])
            got = k.state.record_group(rid)
            if target is None:
                ok = [m for m in k.state.get_group(got).members] == [rid]                            # it started a group of its own
            else:
                ok = got == k._resolve(target)                                                       # it joined the group decide named (or the one that group was merged into)
            if not ok:
                mismatches.append((rid, target, got))
        self.assertEqual(mismatches, [], f"{len(mismatches)} of {len(order)} decisions differ from arrive's")

    def test_reusing_scores_of_unchanged_groups_gives_the_same_answer_as_scoring_everything(self):
        d, raw, order = data()
        k = gs.new_kernel()
        for rid in order[:300]:
            k.arrive(rid, raw[rid])
        for rid in order[300:340]:
            matching, cands, _ = gs.derive(k, raw[rid], "content")
            full = gs.decide(k, matching, cands)
            _, known, _ = full
            reused = gs.decide(k, matching, cands, known)
            self.assertEqual((reused[0], reused[2]), (full[0], 0))                                  # same decision, and nothing had to be scored again


class TestAReusedScoreIsNeverStale(unittest.TestCase):
    def test_every_score_validation_by_re_deciding_reuses_equals_the_score_it_would_get_now(self):
        """The saving of validating by re-deciding is that groups that did not change keep their score. If one that DID change kept its old score, a conflict would be missed."""
        d, raw, order = data()
        real, wrong, reused = gs.decide, [], []

        def checking(k, matching, candidates, known=None):
            result = real(k, matching, candidates, known)
            if known:
                fresh = real(k, matching, candidates)                                                # the same groups, everything scored NOW
                reused.append(len([g for g in candidates if g in known]))
                wrong.extend(g for g in candidates if abs(result[1][g] - fresh[1][g]) > 1e-12)
            return result
        with mock.patch.object(gs, "decide", checking):
            gs.simulate(raw, order, d["updates"], 256, "redecide")
        self.assertGreater(sum(reused), 0, "nothing was reused, so this proved nothing")
        self.assertEqual(wrong, [])


class TestWhatCommitsIsASerialExecution(unittest.TestCase):
    def groups_of(self, k):
        return set(gs.group_sets(k).values())

    def test_replaying_the_commit_order_one_at_a_time_gives_exactly_the_same_groups(self):
        d, raw, order = data()
        for mode in MODES:
            for workers in (8, 64):
                k, _, committed = gs.simulate(raw, order, d["updates"], workers, mode)
                replay = gs.new_kernel()
                for rid in committed:
                    replay.arrive(rid, raw[rid])
                for u in d["updates"]:
                    replay.update(u["rid"], u["raw"])
                self.assertEqual(self.groups_of(k), self.groups_of(replay), f"{mode}, W={workers}")

    def test_the_same_holds_on_top_of_a_base_that_already_exists(self):
        d, raw, order = data()
        for mode in MODES:
            k, _, committed = gs.simulate(raw, order, d["updates"], 32, mode, prefix=200)
            replay = gs.new_kernel()
            for rid in order[:200] + committed:
                replay.arrive(rid, raw[rid])
            for u in d["updates"]:
                replay.update(u["rid"], u["raw"])
            self.assertEqual(self.groups_of(k), self.groups_of(replay), mode)

    def test_every_record_commits_exactly_once(self):
        d, raw, order = data()
        for mode in MODES:
            _, _, committed = gs.simulate(raw, order, d["updates"], 64, mode)
            self.assertEqual(sorted(committed), sorted(order), mode)

    def test_one_worker_is_the_sequential_run_with_no_conflict_at_all(self):
        d, raw, order = data()
        base = gs.group_sets(gs.sequential(raw, order, d["updates"]))
        for mode in MODES:
            k, stats, committed = gs.simulate(raw, order, d["updates"], 1, mode)
            self.assertEqual(stats["conflicts"], 0, mode)
            self.assertEqual(committed, order, mode)
            self.assertEqual(gs.group_sets(k), base, mode)


class TestTheValidation(unittest.TestCase):
    """Hand-made scenarios on the benchmark's own record shape: three records that are all one person."""

    def person(self):
        d, raw, order = data()
        one = next(r["raw"] for r in d["rows"] if r["scenario"] == "background")
        return {"base": one, "x": dict(one), "y": dict(one)}

    def test_a_phantom_two_new_records_of_one_person_read_together_end_up_in_one_group(self):
        raws = self.person()
        for mode in MODES:
            k, stats, committed = gs.simulate(raws, ["x", "y"], [], 2, mode)
            groups = set(gs.group_sets(k).values())
            self.assertEqual(groups, {frozenset({"x", "y"})}, mode)                                  # not two groups: the second saw the first at commit
            self.assertEqual(stats["conflicts"], 1, mode)                                            # the later one to commit was invalidated and redone
            self.assertEqual(stats["necessary"], 1, mode)                                            # and it was NECESSARY: on its stale read it would have started a second group

    def test_a_group_that_changed_without_changing_the_decision_is_a_false_alarm_unless_validation_re_decides(self):
        raws = self.person()
        order = ["base", "x", "y"]
        seen = {}
        for mode in MODES:
            _, stats, _ = gs.simulate(raws, order, [], 2, mode, prefix=1)                          # base exists; x and y both read it and both decide to join it
            seen[mode] = (stats["conflicts"], stats["false_alarms"])
        self.assertEqual(seen["version"], (1, 1))                                                    # x joined, the group changed: y's read is stale, yet y would join anyway
        self.assertEqual(seen["content"], (1, 1))
        self.assertEqual(seen["redecide"], (0, 0))                                                   # re-deciding with the stored scores finds the same answer: nothing to redo

    def test_a_version_is_bumped_by_a_write_so_the_strict_mode_really_sees_a_change(self):
        raws = self.person()
        k = gs.new_kernel()
        k.arrive("base", raws["base"])
        gid = k.state.record_group("base")
        before = k.state.get_group(gid).version
        k.arrive("x", raws["x"])
        self.assertGreater(k.state.get_group(gid).version, before)


class TestTheRedoQueue(unittest.TestCase):
    def test_it_drains_and_a_record_that_will_not_settle_is_committed_alone_not_lost(self):
        d, raw, order = data()
        k, stats, committed = gs.simulate(raw, order, d["updates"], 256, "version", max_rounds=0)       # no retries allowed: every conflict is serialised at once
        self.assertGreater(stats["serialised"], 0)
        self.assertEqual(sorted(committed), sorted(order))
        replay = gs.new_kernel()
        for rid in committed:
            replay.arrive(rid, raw[rid])
        for u in d["updates"]:
            replay.update(u["rid"], u["raw"])
        self.assertEqual(set(gs.group_sets(k).values()), set(gs.group_sets(replay).values()))

    def test_a_run_is_deterministic_for_a_seed_and_the_seed_changes_the_order(self):
        d, raw, order = data()
        a = gs.simulate(raw, order, d["updates"], 64, "redecide", seed=5)
        b = gs.simulate(raw, order, d["updates"], 64, "redecide", seed=5)
        c = gs.simulate(raw, order, d["updates"], 64, "redecide", seed=6)
        self.assertEqual((a[1], a[2]), (b[1], b[2]))
        self.assertNotEqual(a[2], c[2])


class TestTheReport(unittest.TestCase):
    def test_evaluate_reports_the_numbers_the_gate_asks_for(self):
        d, _, _ = data()
        rows = gs.evaluate(d, [8], ["redecide"], "test")
        self.assertEqual(rows[0]["mode"], "sequential")
        r = rows[1]
        for key in ("conflict_rate", "false_alarm_share", "max_tries", "serialised", "delta_f1_points", "effective_parallelism", "records_in_a_different_group_than_sequential"):
            self.assertIn(key, r)
        self.assertLessEqual(abs(r["delta_f1_points"]), 0.5)


if __name__ == "__main__":
    unittest.main()
