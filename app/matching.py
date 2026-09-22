"""
Entity resolution engine for the Customer domain.

Design (matches Section 5.1 of the architecture doc):
  1. Blocking      — cheap candidate generation so we don't compare every
                      record to every other record (O(n^2) doesn't scale).
  2. Deterministic  — exact match on a strong identifier (email or phone)
                      is auto-high-confidence.
  3. Fuzzy scoring  — weighted similarity across name/address fields using
                      Jaro-Winkler string similarity.
  4. Confidence bands — score routes to auto-merge / review queue / no-match.

This module is pure stdlib on purpose (no rapidfuzz/numpy) so it can be
unit-tested and demoed without any pip install. Swapping in rapidfuzz's
`jaro_winkler_similarity` later is a one-line change if you want the C
speedup at scale — see `similarity()` below.
"""
from __future__ import annotations

from dataclasses import dataclass

# --- Confidence thresholds (tenant-configurable in a real deployment) ------
AUTO_MERGE_THRESHOLD = 0.92
REVIEW_THRESHOLD = 0.55  # below this, don't even surface it to a steward


@dataclass
class ScoreBreakdown:
    score: float
    match_type: str
    reasons: list[str]


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
    for names/addresses where typos usually occur later in the string)."""
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
# Blocking — generate a coarse key so only records that *could* match are
# ever compared. In production this backs a DB index / vector ANN lookup;
# here it's an in-memory dict keyed by blocking key.
# ---------------------------------------------------------------------------
def blocking_keys(std: dict) -> set[str]:
    keys = set()
    if std.get("last_name") and std.get("postal_code"):
        keys.add(f"ln+zip:{std['last_name'][:4]}:{std['postal_code']}")
    if std.get("phone"):
        keys.add(f"phone:{std['phone']}")
    if std.get("email"):
        keys.add(f"email:{std['email']}")
    if std.get("last_name") and std.get("first_name"):
        keys.add(f"name:{std['last_name'][:3]}:{std['first_name'][:1]}")
    return keys or {"__no_block__"}  # never silently drop unblockable records


# ---------------------------------------------------------------------------
# Field weights for the fuzzy scorer. Deterministic identifiers (email,
# phone) are handled separately below and short-circuit to a high score.
# ---------------------------------------------------------------------------
_FUZZY_WEIGHTS = {
    "first_name": 0.15,
    "last_name": 0.20,
    "address_line1": 0.20,
    "city": 0.10,
    "postal_code": 0.20,
    "name_semantic": 0.15,  # embedding-based signal, see embeddings.py
}

_embedding_provider = None


def _get_embedding_provider():
    """Lazily instantiated singleton so we don't pay setup cost (or, for
    the sentence-transformers backend, model load time) unless matching
    actually runs."""
    global _embedding_provider
    if _embedding_provider is None:
        from .embeddings import get_provider
        _embedding_provider = get_provider()
    return _embedding_provider


def score_pair(std_a: dict, std_b: dict) -> ScoreBreakdown:
    """Scores two *standardized* customer attribute dicts. Returns a
    confidence score in [0, 1] plus human-readable reasons (for steward
    explainability — never a black-box number alone)."""
    reasons: list[str] = []

    # --- 1. Deterministic checks first ---
    if std_a.get("email") and std_a["email"] == std_b.get("email"):
        reasons.append(f"exact email match ({std_a['email']})")
        return ScoreBreakdown(0.99, "deterministic", reasons)

    if std_a.get("phone") and std_a["phone"] == std_b.get("phone"):
        reasons.append(f"exact phone match ({std_a['phone']})")
        return ScoreBreakdown(0.96, "deterministic", reasons)

    # --- 2. Fuzzy weighted score across remaining fields ---
    total_weight = 0.0
    weighted_sum = 0.0
    for field, weight in _FUZZY_WEIGHTS.items():
        if field == "name_semantic":
            name_a = f"{std_a.get('first_name', '')} {std_a.get('last_name', '')}".strip()
            name_b = f"{std_b.get('first_name', '')} {std_b.get('last_name', '')}".strip()
            if not name_a or not name_b:
                continue
            sim = _get_embedding_provider().similarity(name_a, name_b)
            weighted_sum += sim * weight
            total_weight += weight
            if sim >= 0.6:
                reasons.append(f"name semantically similar ({name_a!r} ~ {name_b!r}, {sim:.2f})")
            continue

        val_a, val_b = std_a.get(field, ""), std_b.get(field, "")
        if not val_a or not val_b:
            continue  # no signal from this field either way
        sim = similarity(val_a, val_b)
        weighted_sum += sim * weight
        total_weight += weight
        if sim >= 0.85:
            reasons.append(f"{field} similar ({val_a!r} ~ {val_b!r}, {sim:.2f})")

    if total_weight == 0:
        return ScoreBreakdown(0.0, "fuzzy", ["insufficient overlapping fields"])

    # Normalize by the weight actually available, but penalize sparse
    # comparisons slightly so two records that only share one weak field
    # don't score as confidently as ones with full field coverage.
    coverage_penalty = total_weight / sum(_FUZZY_WEIGHTS.values())
    score = (weighted_sum / total_weight) * (0.5 + 0.5 * coverage_penalty)

    return ScoreBreakdown(round(score, 4), "fuzzy", reasons)


def classify(score: float) -> str:
    if score >= AUTO_MERGE_THRESHOLD:
        return "auto_merge"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "no_match"


def find_candidate_pairs(records: list[tuple[str, dict]]) -> list[tuple[str, str]]:
    """records: list of (record_id, standardized_attrs).
    Returns deduplicated candidate (id_a, id_b) pairs sharing a blocking key —
    this is the step that keeps matching sub-quadratic at scale."""
    blocks: dict[str, list[str]] = {}
    for rec_id, std in records:
        for key in blocking_keys(std):
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
