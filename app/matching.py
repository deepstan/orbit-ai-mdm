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

from dataclasses import dataclass

from .domains import DomainConfig

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
    PREFIX_LEN = 3
    keys = set()
    for group_idx, group in enumerate(config.blocking_field_groups):
        values = [std.get(f) for f in group]
        if all(values):  # every field in the group must be present to form this key
            key_body = ":".join(v[:PREFIX_LEN] if len(group) > 1 else v for v in values)
            keys.add(f"g{group_idx}:{key_body}")
    return keys or {"__no_block__"}  # never silently drop unblockable records


_embedding_provider = None


def _get_embedding_provider():
    global _embedding_provider
    if _embedding_provider is None:
        from .embeddings import get_provider
        _embedding_provider = get_provider()
    return _embedding_provider


def score_pair(std_a: dict, std_b: dict, config: DomainConfig) -> ScoreBreakdown:
    """Scores two *standardized* attribute dicts for the given domain.
    Returns a confidence score in [0, 1] plus human-readable reasons (for
    steward explainability — never a black-box number alone)."""
    reasons: list[str] = []

    # --- 1. Deterministic checks first ---
    for det_field in config.deterministic_fields:
        val_a = std_a.get(det_field)
        if val_a and val_a == std_b.get(det_field):
            reasons.append(f"exact {det_field} match ({val_a})")
            return ScoreBreakdown(0.97, "deterministic", reasons)

    # --- 2. Fuzzy weighted score across configured fields ---
    total_weight = 0.0
    weighted_sum = 0.0

    for f, weight in config.fuzzy_weights.items():
        val_a, val_b = std_a.get(f, ""), std_b.get(f, "")
        if not val_a or not val_b:
            continue
        sim = similarity(val_a, val_b)
        weighted_sum += sim * weight
        total_weight += weight
        if sim >= 0.85:
            reasons.append(f"{f} similar ({val_a!r} ~ {val_b!r}, {sim:.2f})")

    # --- 3. Semantic embedding signal over the domain's configured fields ---
    if config.semantic_fields:
        text_a = " ".join(std_a.get(f, "") for f in config.semantic_fields).strip()
        text_b = " ".join(std_b.get(f, "") for f in config.semantic_fields).strip()
        if text_a and text_b:
            sim = _get_embedding_provider().similarity(text_a, text_b)
            weighted_sum += sim * config.semantic_weight
            total_weight += config.semantic_weight
            if sim >= 0.6:
                reasons.append(f"semantically similar ({text_a!r} ~ {text_b!r}, {sim:.2f})")

    if total_weight == 0:
        return ScoreBreakdown(0.0, "fuzzy", ["insufficient overlapping fields"])

    ideal_weight_total = sum(config.fuzzy_weights.values()) + config.semantic_weight
    coverage_penalty = total_weight / ideal_weight_total
    score = (weighted_sum / total_weight) * (0.5 + 0.5 * coverage_penalty)

    return ScoreBreakdown(round(score, 4), "fuzzy", reasons)


def classify(score: float) -> str:
    if score >= AUTO_MERGE_THRESHOLD:
        return "auto_merge"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "no_match"


def find_candidate_pairs(records: list[tuple[str, dict]], config: DomainConfig) -> list[tuple[str, str]]:
    """records: list of (record_id, standardized_attrs).
    Returns deduplicated candidate (id_a, id_b) pairs sharing a blocking key —
    this is the step that keeps matching sub-quadratic at scale."""
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
