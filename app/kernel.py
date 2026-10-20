"""
The matching KERNEL: decide where a record belongs by comparing it with GROUPS (entities), never with other records.

This module is the PURE part (no database): how a group looks to the matcher, the scorers, and the comparison of a record with a group. app/kernel_router.py is the part that
reads and writes the product's tables around it; experiments/ holds the prototype and benchmark that measured these rules (docs/kernel-design.md has every number).

THE IDEAS (all measured in experiments/, see docs/kernel-design.md):
  * A GROUP is an entity: its member records, and what matching needs to know about them: the values it has seen for each field (its FEATURE SET) and the golden record's values.
    A record is compared with the group's best-matching value per field, so a second email, a second phone or a spelling variant is found without being the survivor.
  * The scorer is pluggable. CorroboratedScorer is the default: an exact match on an identifier that many OTHER groups hold does not identify anyone (the frequency rule), and a
    different SURNAME does not undo an otherwise certain match when two identifiers and the first name agree (a marriage, not a different person). That second rule is ours: it
    is not from any competitor's documentation, and docs/kernel-design.md has the measurement behind it (38 of 72 real people wrongly split without it, 8 with it).
  * A record that matches two groups joins the better one; two groups whose golden records match each other are merged (Senzing and Reltio do this); settings below.
"""
from __future__ import annotations

import logging
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from . import matching
from .matching import score_pair, similarity

ENGINES = ("kernel", "legacy")
DEFAULT_ENGINE = "kernel"
MERGE_GROUPS = ("auto", "propose", "off")
RANKS = ("idf", "keys")


# ------------------------------------------------------------------------------------------------ settings (environment; a bad value fails loudly, never silently)
def _choice(name: str, allowed: tuple, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value not in allowed:
        raise ValueError(f"{name} must be one of {', '.join(allowed)}; got {raw!r}")
    return value


def _number(name: str, default, cast=int, minimum=0):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = cast(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number; got {raw!r}") from None
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}; got {value}")
    return value


_warned_legacy = False


def engine() -> str:
    """MDM_ENGINE: 'kernel' (groups, the default) or 'legacy' (the pairwise engine, kept for rollback and for comparison)."""
    chosen = _choice("MDM_ENGINE", ENGINES, DEFAULT_ENGINE)
    global _warned_legacy
    if chosen == "legacy" and not _warned_legacy:
        _warned_legacy = True
        logging.getLogger("mdm.kernel").warning("MDM_ENGINE=legacy is DEPRECATED: the pairwise engine is kept only for rollback and comparison, and will be removed once the kernel has been proven on "
                                                "PostgreSQL (docs/legacy-engines.md says what, and when)")
    return chosen


@dataclass(frozen=True)
class Settings:
    merge_groups: str = "auto"          # MDM_MERGE_GROUPS: auto = merge two groups whose golden records match each other (Senzing, Reltio); propose = queue a review instead (Ataccama); off
    max_candidates: int | None = None   # MDM_MAX_CANDIDATES: score only the best N candidate groups (a speed dial: it costs accuracy that grows with the data; unset = all)
    rank: str = "idf"                   # MDM_RANK: how candidate groups are ranked when a limit is set: idf = shared keys weighted by their rarity (measured better), keys = by count
    scorer: str = "corroborated"        # MDM_KERNEL_SCORER: corroborated | frequency | rule
    review_cap: int = 5                 # MDM_REVIEW_CAP: at most this many review items for one record


def settings() -> Settings:
    d = Settings()                                  # the defaults live in ONE place: the fields above
    n = _number("MDM_MAX_CANDIDATES", 0)
    return Settings(merge_groups=_choice("MDM_MERGE_GROUPS", MERGE_GROUPS, d.merge_groups), max_candidates=n or d.max_candidates, rank=_choice("MDM_RANK", RANKS, d.rank),
                    scorer=_choice("MDM_KERNEL_SCORER", ("corroborated", "frequency", "rule"), d.scorer),
                    review_cap=_number("MDM_REVIEW_CAP", d.review_cap, int, 1))


# ------------------------------------------------------------------------------------------------ the scorer interface
@dataclass
class Result:
    score: float
    match_type: str
    reasons: list = field(default_factory=list)
    contributions: list = field(default_factory=list)


class Context:
    """What a scorer may ask about the world besides the two sides: how many OTHER groups hold an identifier value."""

    def __init__(self, holders: dict, exclude=frozenset()):
        self._holders, self._exclude = holders, frozenset(exclude)

    def others_holding(self, f: str, v: str) -> int:
        return len(self._holders.get((f, v), set()) - self._exclude)


class RuleScorer:
    """The pairwise scorer (app/matching.py) applied to a record and a group's view of itself."""

    def __init__(self, config):
        self.config = config

    def score(self, record: dict, view: dict, ctx: Context) -> Result:
        sb = score_pair(record, view, self.config)
        return Result(sb.score, sb.match_type, list(sb.reasons), list(sb.contributions))


class FrequencyAwareScorer(RuleScorer):
    """An identifier that many groups hold does not identify anyone: an exact match on it is capped below auto-merge UNLESS the names corroborate (both first and last name
    present on both sides and agreeing). A record that is only an email, or only a phone, cannot be auto-merged into any of the groups that happen to hold it."""

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


def make_scorer(name: str, config):
    return {"rule": RuleScorer, "frequency": FrequencyAwareScorer, "corroborated": CorroboratedScorer}[name](config)


# ------------------------------------------------------------------------------------------------ a group, as the matcher sees it
@dataclass
class GroupView:
    id: str
    members: list                                   # record ids
    std: dict                                       # the matching form of the group's golden record
    features: dict = field(default_factory=dict)    # field -> Counter(value -> how many members hold it)


def view_fields(config) -> set:
    return set(config.fuzzy_weights) | set(config.mismatch_penalties) | set(config.deterministic_fields)


def build_group_view(config, domain: str, group_id: str, records: list, matching_by_id: dict, cleansed_by_id: dict) -> GroupView:
    """From the members' raw records and their staged forms. The golden record's matching form is taken from the member that WINS each field (survivorship), not by standardizing
    the golden values again: a golden value can be typed (a date, a number) or protected (a hashed identifier), and re-standardizing it could change it or fail."""
    from .pipeline import compute_golden            # a function-level import: pipeline imports this module's router
    decisions, _ = compute_golden(config, domain, records, cleansed_by_id)
    wins = Counter(d.winning_source_record_id for d in decisions.values() if d.winning_source_record_id in matching_by_id)
    lead = max(records, key=lambda r: (wins.get(r.id, 0), r.ingested_at or "", r.source_system or "", r.source_record_id or "")).id if records else None      # a tie is settled by what a record IS, never by an id the run made up
    std = dict(matching_by_id.get(lead, {}))
    for f, d in decisions.items():
        winner = matching_by_id.get(d.winning_source_record_id)
        if winner and f in winner:
            std[f] = winner[f]
    wanted = view_fields(config)
    features: dict = defaultdict(Counter)
    for r in records:
        for f, v in matching_by_id.get(r.id, {}).items():
            if v and f in wanted:
                features[f][v] += 1
    return GroupView(group_id, [r.id for r in records], std, dict(features))


def view_of(group: GroupView, rec: dict, config) -> dict:
    """The group as this record should be compared with it: the golden, except that for each field the group's value NEAREST to the record's is used. An identifier the record
    shares with any member is an exact match, even if the golden record holds another."""
    view = dict(group.std)
    det = set(config.deterministic_fields)
    for f, values in group.features.items():
        v = rec.get(f)
        if not v or v == view.get(f):
            continue
        if f in det:
            if v in values:
                view[f] = v
        elif f in config.fuzzy_weights:
            best = max(values, key=lambda x: (similarity(v, x), values[x], x))
            if similarity(v, best) > similarity(v, view.get(f, "")):
                view[f] = best
    return view


def golden_vs_golden(config, scorer, a: GroupView, b: GroupView, holders: dict) -> Result:
    """Do two groups look like the same entity? Their golden records compared, an identifier both groups hold counting as an exact match."""
    left, right = dict(a.std), dict(b.std)
    for f in config.deterministic_fields:
        common = set(a.features.get(f, ())) & set(b.features.get(f, ()))
        if common:
            left[f] = right[f] = sorted(common)[0]
    return scorer.score(left, right, Context(holders, {a.id, b.id}))


def merged_view(config, a: GroupView, b: GroupView, new_id: str) -> GroupView:
    """The feature set of two groups that have just been merged (the survivor's golden record is kept as the std)."""
    features: dict = defaultdict(Counter)
    for g in (a, b):
        for f, c in g.features.items():
            features[f].update(c)
    return GroupView(new_id, list(dict.fromkeys(a.members + b.members)), dict(a.std), dict(features))
