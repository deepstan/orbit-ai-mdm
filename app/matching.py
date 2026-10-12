"""
Entity resolution engine -- domain-agnostic (Phase 2: Product domain).

Design (matches Section 5.1 of the architecture doc):
  1. Blocking      — cheap candidate generation so we don't compare every
                      record to every other record (O(n^2) doesn't scale).
  2. Deterministic  — exact match on a strong identifier (whichever fields
                      a domain marks `deterministic_fields`) is auto-high-confidence.
  3. Fuzzy scoring  — weighted similarity across a domain's `fuzzy_weights`
                      fields, plus a semantic-embedding signal over
                      whichever fields the domain marks `semantic_fields`.
  4. Confidence bands — score routes to auto-merge / review queue / no-match.

Nothing in this file mentions "first_name" or "sku" by name -- every
domain-specific decision comes from the DomainConfig passed in (see
app/domains.py). That's what let the Product domain plug in without
touching this file.
"""
from __future__ import annotations

import re

from dataclasses import dataclass, field

from .domains import DomainConfig
from .normalizers import group_keys

# --- Confidence thresholds (tenant-configurable in a real deployment) ------
AUTO_MERGE_THRESHOLD = 0.92
REVIEW_THRESHOLD = 0.55  # below this, don't even surface it to a steward

# The layout of the blocking keys stored in the blocking_keys table. These
# are part of what the stored index MEANS: change either and every stored
# key is stale. app/indexing.py folds both into a fingerprint, so changing
# them is detected automatically and ingestion pauses until a reindex runs.
BLOCKING_PREFIX_LEN = 3
BLOCKING_KEY_FORMAT = 1


@dataclass
class ScoreBreakdown:
    score: float
    match_type: str
    reasons: list[str]
    contributions: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Jaro-Winkler similarity (pure Python implementation, 0.0-1.0)
# ---------------------------------------------------------------------------
def _jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0

    match_distance = max(len1, len2) // 2 - 1
    match_distance = max(match_distance, 0)

    s1_matches = [False] * len1
    s2_matches = [False] * len2

    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    transpositions //= 2

    return (
        matches / len1 + matches / len2 + (matches - transpositions) / matches
    ) / 3.0


def jaro_winkler(s1: str, s2: str, prefix_weight: float = 0.1) -> float:
    """Jaro-Winkler: Jaro similarity boosted for shared prefixes (common
    for names/addresses/product titles where typos usually occur later
    in the string)."""
    jaro_sim = _jaro(s1, s2)
    prefix_len = 0
    for c1, c2 in zip(s1[:4], s2[:4]):
        if c1 != c2:
            break
        prefix_len += 1
    return jaro_sim + prefix_len * prefix_weight * (1 - jaro_sim)


def similarity(a: str, b: str) -> float:
    """Similarity used throughout the matcher. Empty-vs-empty is treated as
    'no signal' (0.0), not a false match."""
    if not a or not b:
        return 0.0
    return jaro_winkler(a, b)


# ---------------------------------------------------------------------------
# Blocking — generate coarse keys from a domain's configured field groups so
# only records that *could* match are ever compared. In production this
# backs a DB index / vector ANN lookup; here it's an in-memory dict.
# ---------------------------------------------------------------------------
def blocking_keys(std: dict, config: DomainConfig) -> set[str]:
    """Blocking is deliberately permissive -- a short prefix per field in
    a multi-field group -- because the cost of over-blocking is just an
    extra fuzzy-score comparison, while under-blocking silently drops a
    genuine match before it's ever scored. 3 characters is short enough to
    tolerate nicknames ('Jennifer' -> 'Jen') and still-close ZIP prefixes
    ('73301' -> '73344' share a ZIP3), while still meaningfully narrowing
    the candidate pool at scale."""
    keys = set()
    for group_idx, specs in enumerate(config.keyspecs()):
        unless = config.blocking_unless[group_idx] if group_idx < len(config.blocking_unless) else ()
        if unless and any(std.get(f) for f in unless):
            continue  # a CONDITIONAL group: its (loose) keys exist only for records lacking every field in `unless_any`
        values = [std.get(spec.field) for spec in specs]
        if not all(values):  # every field in the group must be present to form this key
            continue
        for body in group_keys(specs, values, BLOCKING_PREFIX_LEN):
            keys.add(f"g{group_idx}:{body}")
    return keys or {"__no_block__"}  # never silently drop unblockable records


_embedding_provider = None


def _get_embedding_provider():
    global _embedding_provider
    if _embedding_provider is None:
        from .embeddings import get_provider
        _embedding_provider = get_provider()
    return _embedding_provider


# Two PRESENT values this dissimilar are a DISAGREEMENT -- evidence against a match -- rather than merely
# a weak similarity. Measured: unrelated first names score below 0.6 about 90% of the time, but a nickname can too
# (Bob/Robert 0.50) -- which is why given names are canonicalized through a nickname table BEFORE they get here.
DISAGREE_BELOW = 0.6
DETERMINISTIC_SCORE = 0.97


NEAR_DATE_PENALTY = 0.1  # a day/month swap, or a year one out, costs this fraction of the field's mismatch penalty
NEAR_SSN_PENALTY = 0.1   # likewise for an SSN one typo away from the other


def _date_parts(v):
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", v or "")
    if m:
        return int(m[1]), int(m[2]), int(m[3])
    return (int(v), None, None) if re.fullmatch(r"\d{4}", v or "") else None


def date_compare(va: str, vb: str):
    """-> (state, similarity) for two matching-form dates (YYYY-MM-DD, or a bare YYYY). 'near' means a plausible slip: day and month
    swapped, or the year out by one with the same day and month. A bare year agrees with any date in that year and disagrees with
    any other. 'missing' is never 'different'."""
    if not va or not vb:
        return "missing", None
    a, b = _date_parts(va), _date_parts(vb)
    if a is None or b is None:  # not a date form we understand: fall back to string similarity
        sim = similarity(va, vb)
        return ("agree" if sim >= 0.85 else "disagree" if sim < DISAGREE_BELOW else "partial"), sim
    (ya, ma, da), (yb, mb, db) = a, b
    if ma is None or mb is None:  # at least one is a bare year
        return ("agree", 0.9) if ya == yb else ("disagree", 0.3)
    if a == b:
        return "agree", 1.0
    if ya == yb and ma == db and da == mb:
        return "near", 0.9
    if ma == mb and da == db and abs(ya - yb) == 1:
        return "near", 0.8
    return "disagree", similarity(va, vb)


def _share_near(a, b) -> bool:
    return bool(a and b and set(a.split(",")) & set(b.split(",")))


def _compare(va: str, vb: str, exact: bool):
    """-> (state, similarity). 'missing' is NOT 'different': an empty field says nothing either way."""
    if not va or not vb:
        return "missing", None
    if exact:
        return ("agree", 1.0) if va == vb else ("disagree", 0.0)
    sim = similarity(va, vb)
    return ("agree" if sim >= 0.85 else "disagree" if sim < DISAGREE_BELOW else "partial"), sim


def score_pair(std_a: dict, std_b: dict, config: DomainConfig) -> ScoreBreakdown:
    """Scores two *standardized* attribute dicts. Returns a score in [0, 1], human-readable reasons, and a
    per-field breakdown (`contributions`, whose `points` sum to the score) so a steward can see WHY.

    Evidence FOR a match comes from an exact identifier match or from weighted field similarity. Evidence
    AGAINST it comes from `mismatch_penalties`: a field where both values are present and disagree subtracts
    from the score. Missing values contribute nothing either way."""
    reasons: list[str] = []
    contributions: list[dict] = []
    specs = getattr(config, "field_specs", None) or {}
    protected = {f for f, sp in specs.items() if getattr(sp, "protect", "")}  # reasons are stored: no hashes, no values
    dates = set(getattr(config, "mismatch_date", None) or ())

    matched = [f for f in config.deterministic_fields if std_a.get(f) and std_a.get(f) == std_b.get(f)]
    if matched:
        match_type, base = "deterministic", DETERMINISTIC_SCORE
        for i, f in enumerate(matched):
            reasons.append(f"exact {f} match" if f in protected else f"exact {f} match ({std_a[f]})")
            contributions.append({"field": f, "state": "agree", "similarity": 1.0, "points": base if i == 0 else 0.0})
    else:
        match_type = "fuzzy"
        parts = []  # (field, similarity, weight)
        for f, weight in config.fuzzy_weights.items():
            va, vb = std_a.get(f, ""), std_b.get(f, "")
            if not va or not vb:
                contributions.append({"field": f, "state": "missing", "similarity": None, "points": 0.0})
                continue
            sim = date_compare(va, vb)[1] if f in dates else similarity(va, vb)
            parts.append((f, sim, weight))
            if sim >= 0.85:
                reasons.append(f"{f} similar ({va!r} ~ {vb!r}, {sim:.2f})")
        if config.semantic_fields:
            text_a = " ".join(std_a.get(f, "") for f in config.semantic_fields).strip()
            text_b = " ".join(std_b.get(f, "") for f in config.semantic_fields).strip()
            if text_a and text_b:
                sim = _get_embedding_provider().similarity(text_a, text_b)
                parts.append(("semantic:" + "+".join(config.semantic_fields), sim, config.semantic_weight))
                if sim >= 0.6:
                    reasons.append(f"semantically similar ({text_a!r} ~ {text_b!r}, {sim:.2f})")
        total_weight = sum(w for _, _, w in parts)
        if total_weight == 0:
            return ScoreBreakdown(0.0, "fuzzy", ["insufficient overlapping fields"], contributions)
        ideal = sum(config.fuzzy_weights.values()) + config.semantic_weight
        scale = (0.5 + 0.5 * total_weight / ideal) / total_weight  # fewer fields compared -> less confidence
        base = sum(sim * w for _, sim, w in parts) * scale
        for f, sim, w in parts:
            contributions.append({"field": f, "state": "agree" if sim >= 0.85 else "disagree" if sim < DISAGREE_BELOW else "partial",
                                  "similarity": round(sim, 4), "points": round(sim * w * scale, 4)})

    penalty = 0.0
    near_ids = {f for f, sp in specs.items() if getattr(sp, "protect", "") and sp.opt("near")}
    for f, p in config.mismatch_penalties.items():
        is_identifier = f in config.deterministic_fields
        if matched and is_identifier and f not in config.exclusive_fields:
            continue  # a second email/phone differing is normal once something else matched exactly
        if f in dates:
            state, sim = date_compare(std_a.get(f, ""), std_b.get(f, ""))
            fraction = NEAR_DATE_PENALTY
        else:
            state, sim = _compare(std_a.get(f, ""), std_b.get(f, ""), exact=is_identifier or f in config.mismatch_exact)
            fraction = NEAR_SSN_PENALTY
            if state == "disagree" and f in near_ids and _share_near(std_a.get(f + "~near"), std_b.get(f + "~near")):
                state = "near"  # one typo apart: strong-but-fallible evidence, not a contradiction
        if state == "disagree":
            penalty += p
            reasons.append(f"{f} disagrees: -{p:.2f}" if f in protected else f"{f} disagrees ({std_a[f]!r} vs {std_b[f]!r}): -{p:.2f}")
            contributions.append({"field": f, "state": "disagree", "similarity": None if sim is None else round(sim, 4), "points": -p})
        elif state == "near":
            cost = round(p * fraction, 4)
            penalty += cost
            reasons.append(f"{f} nearly agrees: -{cost:.2f}")
            contributions.append({"field": f, "state": "partial", "similarity": None if sim is None else round(sim, 4), "points": -cost})

    return ScoreBreakdown(round(max(0.0, base - penalty), 4), match_type, reasons, contributions)


def classify(score: float) -> str:
    if score >= AUTO_MERGE_THRESHOLD:
        return "auto_merge"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "no_match"


def find_candidate_pairs(records: list[tuple[str, dict]], config: DomainConfig) -> list[tuple[str, str]]:
    """Pure in-memory blocking across a list of (record_id, standardized_attrs)
    pairs -- the original blocking algorithm, still correct and still
    used by the test suite, but NOT what the live ingest path calls
    anymore. `pipeline.ingest_record()` now uses an indexed DB lookup
    (Store.find_candidate_record_ids(), backed by the blocking_keys
    table) instead, since re-fetching and re-blocking every existing
    record in Python on every single ingest call doesn't scale --
    measured causing ~150x+ slowdowns on large datasets before the fix.
    This function is kept for offline/batch analysis (e.g. auditing
    blocking behavior over an in-memory sample without touching the DB)
    and as the simplest reference implementation of the blocking logic."""
    blocks: dict[str, list[str]] = {}
    for rec_id, std in records:
        for key in blocking_keys(std, config):
            blocks.setdefault(key, []).append(rec_id)

    seen: set[tuple[str, str]] = set()
    pairs: list[tuple[str, str]] = []
    for ids in blocks.values():
        if len(ids) < 2:
            continue
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                pair = tuple(sorted((ids[i], ids[j])))
                if pair not in seen:
                    seen.add(pair)
                    pairs.append(pair)
    return pairs
