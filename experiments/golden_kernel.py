"""
PROTOTYPE of the matching kernel: a record is matched ONLY against group GOLDEN records.

  * A group is a set of source records and ONE golden record, built from them by the real survivorship rules (app.pipeline.compute_golden).
  * An arriving record looks up candidate groups by blocking on each group's golden (and on every other identifier value the group has seen), scores itself against
    the golden with the real scorer (app.matching.score_pair), and
      - joins the best group if it scores 'auto_merge' and is clearly better than the runner-up (by `margin`),
      - joins the best AND queues a review item "these two groups may be the same" if two groups score auto_merge and it is not clearly better,
      - starts a new group if the best is only 'review' (and queues a review item) or nothing matches.
    A record that matches two groups never merges them.
  * The golden exposes to matching (default decisions): its surviving values for scoring, PLUS every distinct identifier value any member has (emails, phones) for exact
    matching and for finding candidates.
  * An UPDATE removes the record from its group (the group's golden is rebuilt without it), then matches the new version as a fresh arrival against ALL goldens,
    including the one it just left. Nothing else is regrouped: the system never reshuffles on its own.

Pure and in memory: no database. It exists to measure the design before it replaces anything.
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("MDM_AUDIT_RATE", "0")

from app.domains import get_domain_config  # noqa: E402
from app.matching import blocking_keys, classify, score_pair  # noqa: E402
from app.models import SourceRecord  # noqa: E402
from app.pipeline import compute_golden  # noqa: E402
from app.staging import stage_record  # noqa: E402
from app.standardize import standardize  # noqa: E402


class Group:
    def __init__(self, gid):
        self.id, self.members, self.golden, self.std, self.ids, self.keys = gid, [], {}, {}, {}, set()


def _stamp(n: int) -> str:
    return f"2026-01-01T{n // 3_600_000 % 24:02d}:{n // 60_000 % 60:02d}:{n // 1000 % 60:02d}.{n % 1000:03d}Z"


class Kernel:
    def __init__(self, domain: str = "customer", margin: float = 0.05, bridge: bool = False):
        self.domain, self.margin, self.bridge = domain, margin, bridge
        self.config = get_domain_config(domain)
        self.det = list(self.config.deterministic_fields)
        self.groups: dict[str, Group] = {}
        self.index: dict[str, set] = defaultdict(set)
        self.records: dict[str, SourceRecord] = {}
        self.std: dict[str, dict] = {}
        self.cleansed: dict[str, dict] = {}
        self.rec_group: dict[str, str] = {}
        self.reviews: list[tuple] = []
        self.stats = defaultdict(int)
        self._next_group = 0
        self._clock = 0

    # ------------------------------------------------------------------ the golden
    def _rebuild(self, g: Group) -> None:
        recs = [self.records[r] for r in g.members]
        decisions, _ = compute_golden(self.config, self.domain, recs, {r.id: self.cleansed[r.id] for r in recs})
        g.golden = {f: d.value for f, d in decisions.items()}
        g.std = standardize(self.domain, {f: "" if v is None else str(v) for f, v in g.golden.items()})
        g.ids = {f: {self.std[r].get(f) for r in g.members if self.std[r].get(f)} for f in self.det}
        for k in g.keys:
            self.index[k].discard(g.id)
        keys = set(blocking_keys(g.std, self.config))
        for f in self.det:                         # every other identifier value the group has seen is a way to find it
            for v in g.ids[f] - {g.std.get(f)}:
                keys |= set(blocking_keys({**g.std, f: v}, self.config))
        g.keys = keys
        for k in keys:
            self.index[k].add(g.id)

    def _drop(self, g: Group) -> None:
        for k in g.keys:
            self.index[k].discard(g.id)
        del self.groups[g.id]

    def _new_group(self) -> Group:
        self._next_group += 1
        g = Group(f"g{self._next_group:06d}")
        self.groups[g.id] = g
        return g

    # ------------------------------------------------------------------ one record in
    def arrive(self, rid: str, raw: dict, updated: bool = False) -> str:
        self._clock += 1
        stamp = _stamp(self._clock)
        old = self.records.get(rid)
        self.records[rid] = SourceRecord(source_system="s", source_record_id=rid, attributes=raw, id=rid, tenant_id="t", domain=self.domain,
                                         ingested_at=old.ingested_at if old else stamp, updated_at=stamp if old else None)
        matching, cleansed = stage_record(self.domain, self.config, raw)
        self.std[rid], self.cleansed[rid] = matching, cleansed
        candidates = set()
        for k in blocking_keys(matching, self.config):
            candidates |= self.index.get(k, set())
        self.stats["candidate_groups"] += len(candidates)
        scored = []
        for gid in candidates:
            g = self.groups[gid]
            view = dict(g.std)
            for f in self.det:                      # exact match against ANY identifier value the group has seen
                v = matching.get(f)
                if v and v in g.ids[f]:
                    view[f] = v
            scored.append((score_pair(matching, view, self.config).score, gid))
        scored.sort(key=lambda x: (-x[0], x[1]))     # best first; ties go to the older group
        autos = [(s, g) for s, g in scored if classify(s) == "auto_merge"]
        if autos:
            best_score, best = autos[0]
            if len(autos) > 1 and best_score - autos[1][0] < self.margin:
                self.reviews.append(("groups_may_be_same", best, autos[1][1], round(best_score, 3)))
                self.stats["ambiguous"] += 1
            target = self.groups[best]
        else:
            review = [(s, g) for s, g in scored if classify(s) == "review"]
            target = self._new_group()
            if review:
                self.reviews.append(("record_may_join", rid, review[0][1], round(review[0][0], 3)))
                self.stats["queued_for_review"] += 1
        target.members.append(rid)
        self.rec_group[rid] = target.id
        self._rebuild(target)
        if self.bridge and autos:          # entity-to-entity: the group the record joined, against the other groups it also matched
            for _, other_id in autos:
                if other_id != target.id and other_id in self.groups and other_id != target.id:
                    other = self.groups[other_id]
                    if classify(self._golden_vs_golden(target, other)) == "auto_merge":
                        target = self._merge(target, other)
        return target.id

    def _golden_vs_golden(self, a: Group, b: Group) -> float:
        left, right = dict(a.std), dict(b.std)
        for f in self.det:                  # an identifier the two groups both hold is an exact match
            common = a.ids[f] & b.ids[f]
            if common:
                left[f] = right[f] = sorted(common)[0]
        return score_pair(left, right, self.config).score

    def _merge(self, a: Group, b: Group) -> Group:
        keep, gone = (a, b) if a.id < b.id else (b, a)      # the older group survives
        for r in gone.members:
            keep.members.append(r)
            self.rec_group[r] = keep.id
        self._drop(gone)
        self._rebuild(keep)
        self.stats["groups_merged"] += 1
        return keep

    def update(self, rid: str, raw: dict) -> str:
        """The record leaves its group (the golden is rebuilt without it); the new version is matched like a fresh arrival against every golden."""
        before = self.rec_group[rid]
        g = self.groups[before]
        g.members.remove(rid)
        if g.members:
            self._rebuild(g)
        else:
            self._drop(g)
        after = self.arrive(rid, raw, updated=True)
        self.stats["updates"] += 1
        if after != before:
            self.stats["moved_by_update"] += 1
        return after

    def assign(self) -> dict:
        return dict(self.rec_group)
