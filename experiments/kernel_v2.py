"""
PROTOTYPE of the matching kernel, version 2: the five design decisions from the competitor review, folded in. Pure and in memory, on the real standardizer, blocking and
survivorship, so the comparison with golden_kernel.py (version 1) measures the DESIGN.

  1. A group holds a FEATURE SET: every distinct value of every field, with how many member records hold it. The golden record is a VIEW derived from the members
     (survivorship), not what is matched. A record is scored against the group's best-matching values per field, so a second email, a second phone or a spelling variant
     is found without being the survivor.
  2. A VALUE-FREQUENCY table: which groups hold each identifier value. It finds identifiers that do not identify (held by many groups), and caps over-populated blocking keys.
  3. RELATIONSHIPS between groups that are NOT the same entity: a household (same address and family name, different first name), instead of either a false merge or nothing.
  4. An EVENT OUTBOX: every decision appends an event; replaying the events rebuilds exactly the same groups and links (so downstream systems can subscribe, and the state is auditable).
  5. A pluggable SCORER interface: RuleScorer wraps the current scorer; FrequencyAwareScorer adds the frequency rule; a probabilistic scorer can be dropped in later.

Unchanged from version 1: a record is matched only against groups; a record that matches two groups joins the better one if clearly better (else a review is queued); two groups
whose goldens match each other merge; an UPDATE removes the record from its group and re-matches it as a fresh arrival; nothing else is ever regrouped.
"""
from __future__ import annotations

import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Protocol

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("MDM_AUDIT_RATE", "0")

from app import matching  # noqa: E402
from app.domains import get_domain_config  # noqa: E402
from app.matching import _below, blocking_keys, classify, score_pair, similarity  # noqa: E402
from app.models import SourceRecord  # noqa: E402
from app.pipeline import compute_golden  # noqa: E402
from app.staging import stage_record  # noqa: E402
from app.standardize import standardize  # noqa: E402


# ------------------------------------------------------------------------------------------------ 5. the scorer interface
@dataclass
class Result:
    score: float
    match_type: str
    reasons: list = field(default_factory=list)
    contributions: list = field(default_factory=list)


class Context:
    """What a scorer may ask about the world besides the two sides: how many OTHER groups hold an identifier value."""

    def __init__(self, holders: dict, exclude: frozenset):
        self._holders, self._exclude = holders, exclude

    def others_holding(self, f: str, v: str) -> int:
        return len(self._holders.get((f, v), set()) - self._exclude)


class Scorer(Protocol):
    def score(self, record: dict, view: dict, ctx: Context) -> Result: ...


class RuleScorer:
    """The current scorer, unchanged."""

    def __init__(self, config):
        self.config = config

    def score(self, record: dict, view: dict, ctx: Context) -> Result:
        sb = score_pair(record, view, self.config)
        return Result(sb.score, sb.match_type, list(sb.reasons), list(sb.contributions))


class FrequencyAwareScorer(RuleScorer):
    """An identifier that many groups hold does not identify anyone: an exact match on it is capped below auto-merge UNLESS the names corroborate (both first and last
    name present on both sides and agreeing). A record that is only an email, or only a phone, cannot be auto-merged into any of the groups that happen to hold it."""

    def __init__(self, config, shared_by: int = 3, cap: float = 0.80):
        super().__init__(config)
        self.shared_by, self.cap = shared_by, cap

    @staticmethod
    def _names_corroborate(record: dict, view: dict) -> bool:
        return all(record.get(f) and view.get(f) and similarity(record[f], view[f]) >= 0.85 for f in ("first_name", "last_name"))

    def score(self, record: dict, view: dict, ctx: Context) -> Result:
        r = super().score(record, view, ctx)
        if r.match_type != "deterministic" or self._names_corroborate(record, view):
            return r
        for f in self.config.deterministic_fields:
            v = record.get(f)
            n = ctx.others_holding(f, v) if v and view.get(f) == v else 0
            if n >= self.shared_by and r.score > self.cap:
                r.reasons.append(f"{f} is held by {n} other groups: it does not identify anyone")
                r.score = self.cap
        return r


class CorroboratedScorer(FrequencyAwareScorer):
    """A changed SURNAME must not undo an otherwise certain match. When two identifiers agree (email AND phone) and the first name agrees, a different last name is a changed name
    (marriage, divorce), so the last-name penalty is given back. Two different people who share an email and a phone have different first names, so they stay penalised."""

    def score(self, record: dict, view: dict, ctx: Context) -> Result:
        r = super().score(record, view, ctx)
        if r.match_type != "deterministic" or not any("last_name disagrees" in x for x in r.reasons):
            return r
        agreeing = [f for f in self.config.deterministic_fields if record.get(f) and record.get(f) == view.get(f)]
        if len(agreeing) >= 2 and similarity(record.get("first_name", ""), view.get("first_name", "")) >= 0.85:
            r.score = min(r.score + self.config.mismatch_penalties.get("last_name", 0.0), matching.DETERMINISTIC_SCORE)
            r.reasons.append(f"last_name differs, but {' and '.join(agreeing)} agree and so does the first name: a changed name, not a different person")
        return r


# ------------------------------------------------------------------------------------------------ the kernel
class Group:
    def __init__(self, gid):
        self.id, self.members, self.golden, self.std = gid, [], {}, {}
        self.features: dict[str, Counter] = {}
        self.keys: set = set()
        self.version = 0                # bumped each time the group is saved: the optimistic-concurrency token Gate S will need
        self.held: set = set()          # the (identifier field, value) pairs this group is registered under in the holders table: so a rebuild does not scan the whole table


def _stamp(n: int) -> str:
    return f"2026-01-01T{n // 3_600_000 % 24:02d}:{n // 60_000 % 60:02d}:{n // 1000 % 60:02d}.{n % 1000:03d}Z"


def household_rule(rec: dict, view: dict) -> bool:
    """Same address and family name, but a different first name: two people in one household, not one person."""
    a, b = rec.get("address_line1"), view.get("address_line1")
    if not a or not b or similarity(a, b) < 0.9 or rec.get("postal_code") != view.get("postal_code"):
        return False
    if not rec.get("last_name") or similarity(rec["last_name"], view.get("last_name", "")) < 0.85:
        return False
    fa, fb = rec.get("first_name", ""), view.get("first_name", "")
    return bool(fa and fb and similarity(fa, fb) < 0.75)


class MemoryState:
    """Everything the kernel remembers, in dictionaries. The kernel never touches these dictionaries directly: it goes through the methods below, so SqlState
    (experiments/gate_p/sqlstate.py) can hold the same things in a database, and one algorithm serves both."""

    def __init__(self):
        self.groups: dict[str, Group] = {}
        self.index: dict[str, set] = defaultdict(set)              # blocking key -> the groups under it
        self.holders: dict[tuple, set] = defaultdict(set)          # (identifier field, value) -> the groups that hold it
        self.records: dict[str, SourceRecord] = {}
        self.std: dict[str, dict] = {}
        self.cleansed: dict[str, dict] = {}
        self.rec_group: dict[str, str] = {}
        self.alias: dict[str, str] = {}
        self.link_set: dict[tuple, dict] = {}
        self.events: list[dict] = []                               # the outbox
        self._next_group = 0
        self._clock = 0

    # records
    def get_record(self, rid):
        return self.records.get(rid)

    def put_record(self, rid, sr, std, cleansed):
        self.records[rid], self.std[rid], self.cleansed[rid] = sr, std, cleansed

    def records_data(self, rids):
        return [(self.records[r], self.std[r], self.cleansed[r]) for r in rids]

    def record_group(self, rid):
        return self.rec_group[rid]

    def set_record_group(self, rid, gid):
        self.rec_group[rid] = gid

    # groups
    def get_group(self, gid):
        return self.groups.get(gid)

    def prefetch_groups(self, gids):
        pass

    def add_group(self, g):
        self.groups[g.id] = g

    def save_group(self, g):
        pass                                                       # the object IS the state

    def remove_group(self, gid):
        del self.groups[gid]

    def new_group_id(self):
        self._next_group += 1
        return f"g{self._next_group:06d}"

    def next_clock(self):
        self._clock += 1
        return self._clock

    # the blocking index and the identifier holders
    def prefetch_buckets(self, keys, limit):
        pass

    def bucket(self, key, limit):
        return self.index.get(key)

    def index_update(self, gid, old_keys, new_keys):
        for k in old_keys - new_keys:
            self.index[k].discard(gid)
        for k in new_keys - old_keys:
            self.index[k].add(gid)

    def holder_view(self):
        return self.holders

    def holders_update(self, gid, old_held, new_held):
        for key in old_held - new_held:
            self.holders[key].discard(gid)
        for key in new_held - old_held:
            self.holders[key].add(gid)

    # aliases, links, the outbox
    def resolve(self, gid):
        while gid in self.alias:
            gid = self.alias[gid]
        return gid

    def set_alias(self, gone, keep):
        self.alias[gone] = keep

    def add_link(self, a, b, kind):
        if (a, b, kind) in self.link_set:
            return False
        self.link_set[(a, b, kind)] = {}
        return True

    def link_keys(self):
        return list(self.link_set)

    def emit(self, event_type, **data):
        self.events.append({"seq": len(self.events), "type": event_type, **data})

    def end_arrival(self):
        pass

    def flush(self):
        pass

    def assignments(self):
        return dict(self.rec_group)

    def all_events(self):
        return list(self.events)


class Kernel2:
    def __init__(self, domain: str = "customer", margin: float = 0.05, bridge: bool | None = None, use_features: bool = True, scorer: str = "corroborated",
                 cap: int = 200, relationships: bool = True, alternates: int = 4, scorer_object=None, update_policy: str = "rematch",
                 max_candidates: int | None = None, rank: str = "keys", state=None, merge_groups: str = "auto"):
        if bridge is not None:                                        # the old on/off switch, kept so earlier callers still work
            merge_groups = "auto" if bridge else "off"
        if merge_groups not in ("auto", "propose", "off"):
            raise ValueError(f"merge_groups must be auto, propose or off; got {merge_groups!r}")
        # merge_groups: when a record joins a group and ANOTHER group it matched has a golden record that matches this group's golden record:
        #   auto = merge the two groups (Senzing and Reltio do; the older group's id survives)   propose = queue a 'these groups may be the same' review and merge nothing (Ataccama's rule)   off = nothing
        self.domain, self.margin, self.merge_groups, self.use_features = domain, margin, merge_groups, use_features
        self._proposed: set = set()
        self.cap, self.relationships, self.alternates = cap, relationships, alternates
        self.max_candidates, self.rank = max_candidates, rank   # score only the best `max_candidates` groups; rank: 'keys' = most shared blocking keys, 'idf' = shared keys weighted by their rarity
        self.update_policy = update_policy          # 'rematch': a changed record is matched as a fresh arrival. 'anchored': it STAYS in its group while it still shares an identifier with it
        self.config = get_domain_config(domain)
        self.det = list(self.config.deterministic_fields)
        self.scorer = scorer_object or {"frequency": FrequencyAwareScorer, "corroborated": CorroboratedScorer, "rule": RuleScorer}[scorer](self.config)
        self.view_fields = set(self.config.fuzzy_weights) | set(self.config.mismatch_penalties) | set(self.det)
        self.alt_fields = {f for grp in self.config.blocking_field_groups for f in grp} if use_features else set(self.det)
        self.state = state if state is not None else MemoryState()
        self.reviews: list[tuple] = []
        self.stats = defaultdict(int)

    # the in-memory state's dictionaries, under the names the tests and tools have always used (a SqlState has no such dictionaries)
    groups = property(lambda self: self.state.groups)
    index = property(lambda self: self.state.index)
    holders = property(lambda self: self.state.holders)
    records = property(lambda self: self.state.records)
    std = property(lambda self: self.state.std)
    cleansed = property(lambda self: self.state.cleansed)
    rec_group = property(lambda self: self.state.rec_group)
    alias = property(lambda self: self.state.alias)
    link_set = property(lambda self: self.state.link_set)
    events = property(lambda self: self.state.all_events())

    def _emit(self, event_type: str, **data) -> None:
        self.state.emit(event_type, **data)

    # ------------------------------------------------------------------ 1. features, and the golden as a view
    def _rebuild(self, g: Group) -> None:
        st = self.state
        members = st.records_data(g.members)
        decisions, _ = compute_golden(self.config, self.domain, [sr for sr, _, _ in members], {sr.id: cl for sr, _, cl in members})
        new_golden = {f: d.value for f, d in decisions.items()}
        g.std = standardize(self.domain, {f: "" if v is None else str(v) for f, v in new_golden.items()})
        g.features = defaultdict(Counter)
        for _, std, _ in members:
            for f, v in std.items():
                if v and f in self.view_fields:
                    g.features[f][v] += 1
        old_held = g.held
        g.held = {(f, v) for f in self.det for v in g.features.get(f, ())}
        st.holders_update(g.id, old_held, g.held)
        old_keys = g.keys
        keys = set(blocking_keys(g.std, self.config))
        for f in self.alt_fields:                                    # every other value the group has seen is a way to find it (a few per field)
            for v, _ in g.features.get(f, Counter()).most_common(self.alternates + 1):
                if v != g.std.get(f):
                    keys |= set(blocking_keys({**g.std, f: v}, self.config))
        g.keys = keys
        st.index_update(g.id, old_keys, keys)
        if new_golden != g.golden:
            g.golden = new_golden
            self._emit("golden_changed", group=g.id, values={k: v for k, v in new_golden.items() if v not in (None, "")})
        st.save_group(g)

    def _view(self, rec: dict, g: Group) -> dict:
        """The group as this record should be compared with it: the golden, except that for each field the group's value nearest to the record's is used."""
        view = dict(g.std)
        for f, values in g.features.items():
            v = rec.get(f)
            if not v or v == view.get(f):
                continue
            if f in self.det:
                if v in values:
                    view[f] = v
            elif self.use_features and f in self.config.fuzzy_weights:
                best = max(values, key=lambda x: (similarity(v, x), values[x], x))
                if similarity(v, best) > similarity(v, view.get(f, "")):
                    view[f] = best
        return view

    def _drop(self, g: Group) -> None:
        st = self.state
        st.index_update(g.id, g.keys, set())
        st.holders_update(g.id, g.held, set())
        st.remove_group(g.id)

    def _new_group(self) -> Group:
        g = Group(self.state.new_group_id())
        self.state.add_group(g)
        self._emit("group_created", group=g.id)
        return g

    def _resolve(self, gid: str) -> str:
        return self.state.resolve(gid)

    # ------------------------------------------------------------------ one record in
    def arrive(self, rid: str, raw: dict, prefer: str | None = None) -> str:
        st = self.state
        stamp = _stamp(st.next_clock())
        old = st.get_record(rid) if prefer is not None else None      # only an UPDATE has an earlier version: for a new record the lookup is a certain miss (a wasted round trip on a database)
        sr = SourceRecord(source_system="s", source_record_id=rid, attributes=raw, id=rid, tenant_id="t", domain=self.domain,
                          ingested_at=old.ingested_at if old else stamp, updated_at=stamp if old else None)
        matching, cleansed = stage_record(self.domain, self.config, raw)
        st.put_record(rid, sr, matching, cleansed)
        keys = sorted(blocking_keys(matching, self.config))
        st.prefetch_buckets(keys, self.cap + 1)
        weight = {}
        for k in keys:
            bucket = st.bucket(k, self.cap + 1)
            if not bucket:
                continue
            if len(bucket) > self.cap:                                  # 2. a key that many groups share finds nothing in particular
                self.stats["skipped_keys"] += 1
                continue
            w = 1.0 / len(bucket) if self.rank == "idf" else 1.0
            for gid in bucket:
                weight[gid] = weight.get(gid, 0.0) + w
        self.stats["candidate_groups"] += len(weight)                   # retrieved
        candidates = sorted(weight, key=lambda g: (-weight[g], g))      # best evidence first, ties by group id: deterministic
        if self.max_candidates is not None:
            kept = candidates[: self.max_candidates]
            if prefer in weight and prefer not in kept:                 # a changed record's own group is always looked at
                kept.append(prefer)
            candidates = kept
        self.stats["scored_groups"] += len(candidates)
        st.prefetch_groups(candidates + ([prefer] if prefer else []))
        scored = []
        for gid in candidates:
            view = self._view(matching, st.get_group(gid))
            r = self.scorer.score(matching, view, Context(st.holder_view(), frozenset({gid})))
            scored.append((r.score, gid, view, r))
        scored.sort(key=lambda x: (-x[0], x[1]))
        autos = [t for t in scored if classify(t[0]) == "auto_merge"]
        forced = None
        if prefer is not None and self.update_policy == "anchored" and st.get_group(prefer) is not None:   # a changed record whose identity is still tied to its old group
            old_group = st.get_group(prefer)
            if any(matching.get(f) and matching[f] in old_group.features.get(f, ()) for f in self.det):
                forced = next((t for t in scored if t[1] == prefer and classify(t[0]) != "no_match"), None)
        if forced:
            score, gid, _, r = forced
            target, why = st.get_group(gid), ("kept", round(score, 3), r.reasons[:3])
            if classify(score) == "review":
                self.reviews.append(("kept_after_change", rid, gid, round(score, 3)))
                self._emit("review_queued", kind="kept_after_change", record=rid, groups=[gid], score=round(score, 3))
                self.stats["kept_after_change"] += 1
        elif autos:
            best_score, best, _, r = autos[0]
            if len(autos) > 1 and best_score - autos[1][0] < self.margin:
                self.reviews.append(("groups_may_be_same", best, autos[1][1], round(best_score, 3)))
                self._emit("review_queued", kind="groups_may_be_same", groups=[best, autos[1][1]], score=round(best_score, 3))
                self.stats["ambiguous"] += 1
            target, why = st.get_group(best), ("joined", round(best_score, 3), r.reasons[:3])
        else:
            review = [t for t in scored if classify(t[0]) == "review"]
            target, why = self._new_group(), ("new", 0.0, [])
            if review:
                self.reviews.append(("record_may_join", rid, review[0][1], round(review[0][0], 3)))
                self._emit("review_queued", kind="record_may_join", record=rid, groups=[review[0][1]], score=round(review[0][0], 3))
                self.stats["queued_for_review"] += 1
        target.members.append(rid)
        st.set_record_group(rid, target.id)
        self._emit("record_joined", record=rid, group=target.id, how=why[0], score=why[1], reasons=why[2])
        self._rebuild(target)
        if self.merge_groups != "off":                                  # entity-to-entity: the group joined, against the other groups the record also matched
            for _, other_id, _, _ in autos:
                other = st.get_group(other_id) if other_id != target.id else None
                if other is None:
                    continue
                score = self._golden_vs_golden(target, other)
                if classify(score) != "auto_merge":
                    continue
                if self.merge_groups == "auto":
                    target = self._merge(target, other)
                elif (pair := tuple(sorted((target.id, other.id)))) not in self._proposed:
                    self._proposed.add(pair)
                    self.reviews.append(("groups_may_be_same", pair[0], pair[1], round(score, 3)))
                    self._emit("review_queued", kind="groups_may_be_same", groups=list(pair), score=round(score, 3))
                    self.stats["merge_proposed"] += 1
        if self.relationships:                                          # 3. groups that are NOT the same entity but are related
            for score, gid, view, _ in scored:
                gid = self._resolve(gid)
                if gid != target.id and st.get_group(gid) is not None and household_rule(matching, view):
                    self._link(target.id, gid, "household")
        st.end_arrival()
        return target.id

    def _link(self, a: str, b: str, kind: str) -> None:
        a, b = sorted((self._resolve(a), self._resolve(b)))
        if a != b and self.state.add_link(a, b, kind):
            self._emit("link_added", a=a, b=b, link=kind)
            self.stats["links"] += 1

    def _golden_vs_golden(self, a: Group, b: Group) -> float:
        left, right = dict(a.std), dict(b.std)
        for f in self.det:
            common = set(a.features.get(f, ())) & set(b.features.get(f, ()))
            if common:
                left[f] = right[f] = sorted(common)[0]
        return self.scorer.score(left, right, Context(self.state.holder_view(), frozenset({a.id, b.id}))).score

    def _merge(self, a: Group, b: Group) -> Group:
        keep, gone = (a, b) if a.id < b.id else (b, a)                  # the older group survives
        for r in gone.members:
            keep.members.append(r)
            self.state.set_record_group(r, keep.id)
        self.state.set_alias(gone.id, keep.id)
        self._drop(gone)
        self._emit("groups_merged", kept=keep.id, gone=gone.id)
        self._rebuild(keep)
        self.stats["groups_merged"] += 1
        return keep

    def update(self, rid: str, raw: dict) -> str:
        """The record leaves its group (the golden is rebuilt without it); the new version is matched like a fresh arrival against every group."""
        st = self.state
        before = st.record_group(rid)
        g = st.get_group(before)
        g.members.remove(rid)
        self._emit("record_left", record=rid, group=before)
        if g.members:
            self._rebuild(g)
        else:
            self._drop(g)
            self._emit("group_removed", group=before)
        after = self.arrive(rid, raw, prefer=before)
        self.stats["updates"] += 1
        self.stats["moved_by_update"] += after != before
        return after

    # ------------------------------------------------------------------ what the world sees
    def assign(self) -> dict:
        self.state.flush()
        return self.state.assignments()

    def links(self, kind: str | None = None) -> set:
        self.state.flush()
        out = set()
        for (a, b, k) in self.state.link_keys():
            a, b = self._resolve(a), self._resolve(b)
            if a != b and self.state.get_group(a) is not None and self.state.get_group(b) is not None and (kind is None or k == kind):
                out.add(frozenset((a, b)))
        return out


def replay(events: list) -> tuple:
    """Rebuild the groups and links from the outbox alone: (record -> group, set of linked group pairs). If this differs from the kernel's state the outbox is lying."""
    assign, alias, links = {}, {}, set()
    resolve = lambda g: resolve(alias[g]) if g in alias else g
    for e in events:
        t = e["type"]
        if t == "record_joined":
            assign[e["record"]] = e["group"]
        elif t == "record_left":
            assign.pop(e["record"], None)
        elif t == "groups_merged":
            alias[e["gone"]] = e["kept"]
            for r, g in list(assign.items()):
                if g == e["gone"]:
                    assign[r] = e["kept"]
        elif t == "link_added":
            links.add((e["a"], e["b"]))
    live = set(assign.values())
    return assign, {frozenset((resolve(a), resolve(b))) for a, b in links if resolve(a) != resolve(b) and resolve(a) in live and resolve(b) in live}
