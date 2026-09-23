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
    """Jaro string similarity between two strings, from 0.0 to 1.0.

    Counts characters that appear in both strings within a sliding window
    (half the longer length, minus one) and how many of those are out of
    order (transpositions). 1.0 means identical; 0.0 means nothing in common
    or one string is empty. Used by ``jaro_winkler``.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        float: Similarity in ``[0.0, 1.0]``.
    """
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
    in the string).

    The boost is ``prefix_len * prefix_weight * (1 - jaro)``, where
    ``prefix_len`` is the length of the shared prefix, capped at 4
    characters.

    Args:
        s1: First string.
        s2: Second string.
        prefix_weight: How much each shared prefix character adds. Defaults
            to 0.1, the standard value; keep it at or below 0.25 so the
            result stays within 1.0.

    Returns:
        float: Similarity in ``[0.0, 1.0]``.

    Example:
        ``jaro_winkler("martha", "marhta")`` -> about ``0.961``
    """
    jaro_sim = _jaro(s1, s2)
    prefix_len = 0
    for c1, c2 in zip(s1[:4], s2[:4]):
        if c1 != c2:
            break
        prefix_len += 1
    return jaro_sim + prefix_len * prefix_weight * (1 - jaro_sim)


def similarity(a: str, b: str) -> float:
    """Similarity used throughout the matcher. Empty-vs-empty is treated as
    'no signal' (0.0), not a false match.

    Args:
        a: First (standardized) value.
        b: Second (standardized) value.

    Returns:
        float: Jaro-Winkler similarity in ``[0.0, 1.0]``, or ``0.0`` if
        either value is empty.
    """
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
    the candidate pool at scale.

    How keys are built: for each field group in
    ``config.blocking_field_groups`` where EVERY field has a value, one key
    is made. Single-field groups use the full value (e.g. the whole email);
    multi-field groups join a 3-character prefix of each value. Keys are
    prefixed with the group number (``"g0:"``, ``"g1:"``...) so values from
    different groups never collide.

    If no group can be built, the record gets the special key
    ``"__no_block__"``. All such records land in one shared block and are
    compared with each other, so they are never silently dropped.

    Args:
        std: Standardized attributes of one record.
        config: The domain's configuration.

    Returns:
        set[str]: One or more blocking keys.

    Example:
        Customer with last_name ``"smith"`` and postal_code ``"94107"`` gets
        ``"g0:smi:941"`` among its keys.
    """
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
    """Return the shared embedding provider, creating it on first use.

    The provider is cached in a module-level variable, so it's built only
    once per process. ``embeddings`` is imported lazily here so importing
    ``matching`` stays cheap.

    Returns:
        The provider returned by ``embeddings.get_provider()``.
    """
    global _embedding_provider
    if _embedding_provider is None:
        from .embeddings import get_provider
        _embedding_provider = get_provider()
    return _embedding_provider


def score_pair(std_a: dict, std_b: dict, config: DomainConfig) -> ScoreBreakdown:
    """Scores two *standardized* attribute dicts for the given domain.
    Returns a confidence score in [0, 1] plus human-readable reasons (for
    steward explainability — never a black-box number alone).

    Scoring steps:

    1. Deterministic: for each field in ``config.deterministic_fields``,
       if both records have the same non-empty value, return immediately
       with score 0.97 and match_type ``"deterministic"``.
    2. Fuzzy: for each field in ``config.fuzzy_weights`` where BOTH sides
       have a value, add ``similarity * weight``. Fields with similarity
       0.85 or higher are listed in the reasons.
    3. Semantic: if the domain has ``semantic_fields``, join those fields
       into one text per record and add the embedding similarity times
       ``config.semantic_weight``. Listed in reasons if 0.6 or higher.
    4. Final score = weighted average of the fields compared, multiplied by
       ``0.5 + 0.5 * coverage``, where coverage is the share of the total
       possible weight that could actually be compared. Records that share
       few filled-in fields are pulled down, by at most half.

    Args:
        std_a: Standardized attributes of the first record.
        std_b: Standardized attributes of the second record.
        config: The domain's configuration (fields and weights).

    Returns:
        ScoreBreakdown: ``score`` (0-1, rounded to 4 places), ``match_type``
        (``"deterministic"`` or ``"fuzzy"``) and ``reasons``. If no field
        could be compared, the score is 0.0 with reason
        ``"insufficient overlapping fields"``.
    """
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
    """Turn a match score into a routing decision.

    Args:
        score: Match score from ``score_pair`` (0-1).

    Returns:
        str: ``"auto_merge"`` if score >= ``AUTO_MERGE_THRESHOLD`` (0.92),
        ``"review"`` if score >= ``REVIEW_THRESHOLD`` (0.55), otherwise
        ``"no_match"``.
    """
    if score >= AUTO_MERGE_THRESHOLD:
        return "auto_merge"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "no_match"


def find_candidate_pairs(records: list[tuple[str, dict]], config: DomainConfig) -> list[tuple[str, str]]:
    """records: list of (record_id, standardized_attrs).
    Returns deduplicated candidate (id_a, id_b) pairs sharing a blocking key —
    this is the step that keeps matching sub-quadratic at scale.

    Every record is put into a bucket for each of its blocking keys; within
    each bucket, every pair of records is produced. A pair that shares
    several keys is only returned once. Each pair is sorted by ID, so
    ``(a, b)`` and ``(b, a)`` count as the same pair.

    Args:
        records: List of ``(record_id, standardized_attrs)`` tuples.
        config: The domain's configuration (used for blocking keys).

    Returns:
        list[tuple[str, str]]: Unique ``(id_a, id_b)`` pairs with
        ``id_a < id_b``, in first-seen order.

    Note:
        A very common blocking key (a big bucket) still produces every pair
        inside it, so the worst case is still quadratic within that bucket.
    """
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
