"""
A learned pair scorer that runs in PURE PYTHON.

The model is trained OFFLINE (train_scorer.py, which needs scikit-learn and numpy) and exported as plain JSON: a list of decision trees. This module
evaluates those trees with nothing but the standard library, so the runtime gains no dependency. A test checks the result against scikit-learn's own
output, to the last digit.

What it scores: a pair's EVIDENCE -- the per-field breakdown (agree / disagree / partial / missing, similarity, points) that scoring already stores on every
candidate, plus the rule score and the match type. `evidence_features` builds the model's inputs from that evidence, and it is the ONLY place that does:
training and serving call the same function, so they cannot drift apart.

The policy lives in the model file beside the trees. The rule score's automatic merges are left alone; the model's say is in PROMOTING a pair out of review, and a
promotion threshold is written there only when it is CERTIFIED: when audits of review-band pairs show, with the stated confidence, that merging at that threshold
stays within the target false-merge rate. Until then the model behaves exactly like the rule score, and can only order and explain the review queue.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from array import array
from pathlib import Path

FORMAT = 1


# --------------------------------------------------------------------------- the model's inputs
def evidence_features(score: float, match_type: str, contributions) -> dict:
    """{feature name: value}. A feature that is absent is 0, which is how training treats it too (DictVectorizer)."""
    features = {"score": float(score), "type:" + str(match_type): 1.0}
    for c in contributions or ():
        name = c["field"]
        features[f"{name}:{c.get('state')}"] = 1.0
        similarity = c.get("similarity")
        features[f"{name}:sim"] = float(similarity) if similarity is not None else -1.0
        features[f"{name}:pts"] = float(c.get("points") or 0.0)
    return features


# --------------------------------------------------------------------------- the scorer
class TreeScorer:
    """Gradient-boosted trees for a binary label, as exported by train_scorer.py. raw = init + learning_rate * sum(tree values); p = sigmoid(raw)."""

    def __init__(self, doc: dict):
        if doc.get("format") != FORMAT:
            raise ValueError(f"unsupported scorer format {doc.get('format')!r} (this code reads format {FORMAT})")
        self.doc = doc
        self.names = list(doc["feature_names"])
        self._index = {name: i for i, name in enumerate(self.names)}
        self.init_raw = float(doc["init_raw"])
        self.learning_rate = float(doc["learning_rate"])
        self._trees = [(t["left"], t["right"], t["feature"], t["threshold"], t["value"]) for t in doc["trees"]]
        self.policy = dict(doc.get("policy") or {})
        self.model_id = doc["model_id"]

    @classmethod
    def from_json(cls, text: str) -> "TreeScorer":
        return cls(json.loads(text))

    def to_json(self) -> str:
        return json.dumps(self.doc, separators=(",", ":"))

    def vector(self, features: dict):
        """The dense inputs in the model's order. They are rounded to 32-bit floats, as scikit-learn does before it compares a value with a threshold."""
        x = [0.0] * len(self.names)
        for name, value in features.items():
            i = self._index.get(name)
            if i is not None:
                x[i] = value
        return array("f", x)

    def raw(self, features: dict) -> float:
        x = self.vector(features)
        total = self.init_raw
        for left, right, feature, threshold, value in self._trees:
            node = 0
            while left[node] != -1:
                node = left[node] if x[feature[node]] <= threshold[node] else right[node]
            total += self.learning_rate * value[node]
        return total

    def probability(self, features: dict) -> float:
        raw = self.raw(features)
        return 1.0 / (1.0 + math.exp(-raw)) if raw >= 0 else math.exp(raw) / (1.0 + math.exp(raw))

    def score_candidate(self, score: float, match_type: str, contributions) -> float:
        """The probability that a pair is the same, from what scoring already stored on it."""
        return self.probability(evidence_features(score, match_type, contributions))

    def decide(self, p: float, rule_decision: str | None = None, uncertified_ok: bool = False) -> str:
        return decide(p, self.policy, rule_decision, uncertified_ok)


def decide(p: float, policy: dict, rule_decision: str | None = None, uncertified_ok: bool = False) -> str:
    """What happens to a pair: 'auto_merge' or 'review' ('drop' only if a drop threshold is set, and none is by default: one learned on another tenant lost 3.8% of
    the real duplicates).

    The rule score's automatic merges STAY: the model never overrides them, so a model with no certificate behaves exactly like the rule score. The model's say is
    in PROMOTING a pair the rule score sent to review, and only at or above `promote_threshold`, which is certified on its own: audits of review-band pairs
    must show, at the stated confidence, that merging them stays within the target. For a shadow report an uncertified estimate may be asked for; it is never acted on."""
    if rule_decision == "auto_merge":
        return "auto_merge"
    threshold = policy.get("promote_threshold")
    if threshold is None and uncertified_ok:
        threshold = policy.get("promote_threshold_uncertified")
    if threshold is not None and p >= threshold:
        return "auto_merge"
    drop = policy.get("drop_threshold")
    if drop is not None and p < drop:
        return "drop"
    return "review"


PROMOTED_BY = "system:learned_scorer"   # decided_by of a pair the model promoted out of review: how the audit tells the two kinds of automatic merge apart
AUDIT_RATE_ENV, DEFAULT_AUDIT_RATE = "MDM_AUDIT_RATE", 0.02
AUDIT_RATE_PROMOTED_ENV, DEFAULT_AUDIT_RATE_PROMOTED = "MDM_AUDIT_RATE_PROMOTED", 0.10


def _rate_from_environment(name: str, default: float) -> float:
    try:
        return min(1.0, max(0.0, float(os.environ.get(name, default))))
    except ValueError:
        return default


def audit_rate(promoted: bool = False) -> float:
    """The share of automatic merges flagged for a person to check. The rule score's merges: 2% (MDM_AUDIT_RATE). The model's PROMOTIONS: 10% (MDM_AUDIT_RATE_PROMOTED).
    They are the merges the rule score would not have made, there are far fewer of them, and the rollback needs 100 audited promotions before it acts: at 2% that would
    take 5,000 promotions, and a model that promotes 2,000 pairs would never be watched; at 10% it takes 1,000.

    MDM_AUDIT_RATE=0 switches ALL auditing off, promotions included, unless MDM_AUDIT_RATE_PROMOTED is set explicitly. A higher MDM_AUDIT_RATE raises the promotions' rate to match."""
    base = _rate_from_environment(AUDIT_RATE_ENV, DEFAULT_AUDIT_RATE)
    if not promoted:
        return base
    if AUDIT_RATE_PROMOTED_ENV in os.environ:
        return _rate_from_environment(AUDIT_RATE_PROMOTED_ENV, DEFAULT_AUDIT_RATE_PROMOTED)
    if AUDIT_RATE_ENV in os.environ and base == 0:
        return 0.0
    return max(DEFAULT_AUDIT_RATE_PROMOTED, base)


def audit_sampled(candidate_id: str, rate: float | None = None, promoted: bool = False) -> bool:
    """Whether this merge is in the audit sample. Decided by a hash of the pair's id, so it is uniform, needs no state, and is the same whichever engine
    wrote the pair and in whatever order."""
    rate = audit_rate(promoted) if rate is None else rate
    return rate > 0 and int(hashlib.md5(candidate_id.encode()).hexdigest()[:8], 16) / 2 ** 32 < rate


def model_path(tenant_id: str, domain: str) -> Path:
    return Path(os.environ.get("MDM_MODEL_DIR", "models")) / f"{tenant_id}__{domain}_scorer.json"


_cache: dict = {}


def load_scorer(tenant_id: str, domain: str):
    """The tenant's exported scorer, or None. Re-read when the file changes."""
    path = model_path(tenant_id, domain)
    if not path.exists():
        return None
    stamp = (str(path), path.stat().st_mtime_ns)
    if stamp not in _cache:
        _cache.clear()
        _cache[stamp] = TreeScorer.from_json(path.read_text())
    return _cache[stamp]


def model_id_of(doc_without_id: dict) -> str:
    return hashlib.sha256(json.dumps(doc_without_id, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]


# --------------------------------------------------------------------------- the statistics a certification rests on
def upper_bound(false: int, n: int, confidence: float = 0.95) -> float:
    """One-sided Clopper-Pearson upper bound on a false rate: the largest rate p for which seeing `false` or fewer errors in `n` is still not
    less likely than (1 - confidence). With no errors it is 1 - (1 - confidence) ** (1 / n): about 3 / n, which is why proving one error in
    2,000 takes about 6,000 audited merges. Pure Python (binomial tail by bisection)."""
    if n <= 0:
        return 1.0
    if false >= n:
        return 1.0
    alpha = 1.0 - confidence
    if false == 0:
        return 1.0 - alpha ** (1.0 / n)
    log_choose = [math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1) for k in range(false + 1)]

    def tail(p: float) -> float:  # P(X <= false) for X ~ Binomial(n, p)
        if p <= 0:
            return 1.0
        if p >= 1:
            return 0.0
        lp, lq = math.log(p), math.log1p(-p)
        return sum(math.exp(log_choose[k] + k * lp + (n - k) * lq) for k in range(false + 1))
    low, high = false / n, 1.0
    for _ in range(80):
        mid = (low + high) / 2
        if tail(mid) > alpha:
            low = mid
        else:
            high = mid
    return high


def lower_bound(false: int, n: int, confidence: float = 0.95) -> float:
    """One-sided Clopper-Pearson LOWER bound on a false rate: how low it can plausibly be, given `false` errors in `n`. Zero errors give 0. The rollback acts on this
    side: it needs evidence that merging is too error-prone, not the absence of evidence that it is safe (which a small sample can never give)."""
    if n <= 0 or false <= 0:
        return 0.0
    alpha = 1.0 - confidence
    if false >= n:
        return alpha ** (1.0 / n)
    log_choose = [math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1) for k in range(false)]

    def cdf(p: float) -> float:  # P(X <= false - 1)
        lp, lq = math.log(p), math.log1p(-p)
        return sum(math.exp(log_choose[k] + k * lp + (n - k) * lq) for k in range(false))
    low, high = 1e-12, false / n
    for _ in range(80):
        mid = (low + high) / 2
        if cdf(mid) > 1.0 - alpha:
            low = mid
        else:
            high = mid
    return low


def certify_threshold(scored, target: float, confidence: float = 0.95):
    """scored: [(probability, is_correct)] for pairs a PERSON checked, chosen at random from the region in question. Returns (threshold, details): the lowest
    threshold t such that, among checked pairs with p >= t, the upper confidence bound on the false rate is within `target`; or (None, details) when no
    threshold can be certified with the evidence so far."""
    ordered = sorted(scored, key=lambda s: -s[0])
    best, false = None, 0
    for k, (p, correct) in enumerate(ordered, start=1):
        false += 0 if correct else 1
        if k < len(ordered) and ordered[k][0] == p:  # a threshold cannot split pairs that tie
            continue
        bound = upper_bound(false, k, confidence)
        if bound <= target:
            best = (p, {"checked_at_or_above": k, "false": false, "upper_bound": bound})
    if best is None:
        n = len(ordered)
        return None, {"checked": n, "false": sum(1 for _p, c in ordered if not c), "needed": "about %d checked pairs with no error" % math.ceil(math.log(1 - confidence) / math.log(1 - target)) if 0 < target < 1 else None}
    return best


# --------------------------------------------------------------------------- the rollback switch
def state_path(tenant_id: str, domain: str) -> Path:
    return model_path(tenant_id, domain).with_name(f"{tenant_id}__{domain}_scorer_state.json")


def read_state(tenant_id: str, domain: str) -> dict:
    path = state_path(tenant_id, domain)
    return json.loads(path.read_text()) if path.exists() else {"status": "active"}


def write_state(tenant_id: str, domain: str, status: str, reason: str = "", details: dict | None = None) -> dict:
    """'active' or 'rolled_back'. Written atomically; the engines read it on every record, so a rollback takes effect on the next one."""
    state = {"status": status, "reason": reason, "at": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()), "details": details or {}}
    path = state_path(tenant_id, domain)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=1))
    os.replace(temporary, path)
    return state


_active_cache: dict = {}


def active_scorer(tenant_id: str, domain: str):
    """The scorer the engines consult: the tenant's model if it exists, has a LIVE promotion threshold, and has not been rolled back. None otherwise, and then
    nothing changes: the rule score alone decides."""
    model, state = model_path(tenant_id, domain), state_path(tenant_id, domain)
    if not model.exists():
        return None
    stamp = (str(model), model.stat().st_mtime_ns, state.stat().st_mtime_ns if state.exists() else 0)
    if stamp not in _active_cache:
        _active_cache.clear()
        scorer = TreeScorer.from_json(model.read_text())
        live = scorer.policy.get("promote_threshold") is not None and read_state(tenant_id, domain).get("status", "active") == "active"
        _active_cache[stamp] = scorer if live else None
    return _active_cache[stamp]


def promote(scorer, score: float, match_type: str, contributions):
    """For a pair the rule score sent to review: (True, p) if the model promotes it to an automatic merge, else (False, p)."""
    p = scorer.score_candidate(score, match_type, contributions)
    return p >= scorer.policy["promote_threshold"], p


def select_promotion_threshold(labeled_review, population_p, n_rule_auto: int, rule_false: int, rule_audited: int, target: float, margin: float = 1.0, min_labeled: int = 100,
                               promoted_cap: float = 10.0):
    """Policy B. The lowest probability at which promoting review pairs keeps the POOLED estimated false rate of ALL automatic merges within target * margin.

    pooled(t) = (N_rule * r_rule + N_promoted(t) * r_promoted(t)) / (N_rule + N_promoted(t))
      r_rule       the audited false rate of the rule score's own merges (smoothed: (false + 0.5) / (audited + 1))
      r_promoted   the labelled false rate of review pairs the model scores at or above t (same smoothing)
      N_promoted   how many review pairs the model scores at or above t

    labeled_review: [(probability, is_correct)] for review-band pairs a person labelled, out of fold. population_p: the model's probability for every pending review pair.
    The pooled estimate alone would let a handful of very bad promotions hide among thousands of good rule merges, so the promoted pairs must ALSO be good on their own:
    their estimated false rate may not exceed `promoted_cap` times the target (10x by default).

    A point estimate, not a certificate: the audits that keep running afterwards, and the rollback, are what guard it. Returns (threshold, details) or (None, details)."""
    import bisect
    if rule_audited <= 0 or n_rule_auto <= 0:
        return None, {"why": "no audited automatic merges yet: the rule score's own false rate is unknown, and promotion is judged against it"}
    r_rule = (rule_false + 0.5) / (rule_audited + 1)
    pop = sorted(population_p)
    ordered = sorted(labeled_review, key=lambda x: -x[0])
    best, false = None, 0
    for k, (p, correct) in enumerate(ordered, start=1):
        false += 0 if correct else 1
        if k < len(ordered) and ordered[k][0] == p:
            continue
        if k < min_labeled:
            continue
        promoted = len(pop) - bisect.bisect_left(pop, p)
        r_promoted = (false + 0.5) / (k + 1)
        pooled = (n_rule_auto * r_rule + promoted * r_promoted) / (n_rule_auto + promoted)
        if pooled <= target * margin and r_promoted <= promoted_cap * target:
            best = (p, {"labeled_at_or_above": k, "false": false, "promoted_population": promoted, "rule_false_rate_estimate": r_rule,
                        "promoted_false_rate_estimate": r_promoted, "pooled_estimate": pooled, "limit": target * margin, "promoted_rate_limit": promoted_cap * target})
    if best is None:
        floor = (n_rule_auto * r_rule) / n_rule_auto
        return None, {"why": f"no threshold keeps the pooled estimate within {target * margin:.4%} with the promoted pairs themselves under {promoted_cap * target:.4%}", "rule_false_rate_estimate": r_rule, "pooled_with_no_promotion": floor,
                      "labeled_review_pairs": len(ordered)}
    return best
