#!/usr/bin/env python3
"""
Trains the learned pair scorer OFFLINE and exports it as plain JSON that the runtime evaluates in pure Python (app/learned_scorer.py).
Needs scikit-learn and numpy: only this script does, the runtime does not.

    python3 train_scorer.py --tenant acme --domain customer                 # from the database: human labels only
    python3 train_scorer.py --tenant acme --domain customer --target 0.0005 # at most 1 false merge in 2,000 automatic merges (the default)

    # a synthetic world, labels from the generator's truth (see benchmarks/profiling/extract_pairs.py)
    python3 train_scorer.py --pairs /tmp/pairs.pkl --regime realistic --audit-rate 0.02 --out /tmp/scorer.json

WHAT IT LEARNS FROM: only what a person decided (Store.human_labeled_candidates): a steward's confirm or reject, and the verdict of someone who checked an
automatic merge. The system's own merges are not labels.

WHAT IT WILL NOT DO: it writes an automatic-merge threshold into the model only when it is CERTIFIED, meaning pairs that a person audited show, at the
stated confidence, that merging automatically above it stays within the target false rate. Until then the file carries only an uncertified estimate, which
a shadow report may show but the runtime never acts on. It never sets a drop threshold: one learned on another tenant lost 3.8% of the real duplicates.
"""
from __future__ import annotations

import argparse
import hashlib
import math
import pickle
import random
import sys
import time

from app import learned_scorer as ls
from app.matching import AUTO_MERGE_THRESHOLD as AUTO_MERGE_SCORE


class NotEnoughLabels(RuntimeError):
    pass


class ExportMismatch(RuntimeError):
    pass


def _group_id(value) -> int:
    return int(hashlib.md5(str(value).encode()).hexdigest()[:8], 16)


def export_body(model, feature_names: list, learning_rate: float) -> dict:
    """A fitted scikit-learn GradientBoostingClassifier (binary) as plain data: per tree the left and right child, feature, threshold and leaf value."""
    prior = float(model.init_.class_prior_[1])
    trees = []
    for estimator in model.estimators_[:, 0]:
        t = estimator.tree_
        trees.append({"left": t.children_left.tolist(), "right": t.children_right.tolist(), "feature": t.feature.tolist(), "threshold": t.threshold.tolist(),
                      "value": t.value[:, 0, 0].tolist()})
    body = {"format": ls.FORMAT, "kind": "gradient_boosted_trees", "feature_names": list(feature_names), "init_raw": math.log(prior / (1 - prior)),
            "learning_rate": learning_rate, "trees": trees}
    return body


def train(examples: list, *, target: float = 0.0005, confidence: float = 0.95, min_labels: int = 200, seed: int = 0, folds: int = 5,
          n_estimators: int = 150, max_depth: int = 4, learning_rate: float = 0.1, subsample: float = 0.8,
          population: list | None = None, n_rule_auto: int = 0, margin: float = 1.0, activate: bool = False):
    """examples: [{"features": {...}, "label": 0|1, "source": "steward"|"audit", "group": anything}]. Returns (document, report). Pairs in one group (one person,
    or one connected set of records) never straddle a training and a test fold, so the out-of-fold predictions are honest."""
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.model_selection import GroupKFold

    n = len(examples)
    y = np.array([e["label"] for e in examples], dtype=int)
    if n < min_labels or int(y.sum()) < 20 or int(n - y.sum()) < 20:
        raise NotEnoughLabels(f"{n} human labels ({int(y.sum())} same, {int(n - y.sum())} different): need at least {min_labels}, and at least 20 of each kind. "
                              f"Steward decisions on the review queue, and audits of automatic merges, are the labels.")
    vec = DictVectorizer(sparse=False)
    X = vec.fit_transform([e["features"] for e in examples]).astype(np.float32).astype(np.float64)  # trees compare 32-bit values
    groups = np.array([_group_id(e["group"]) for e in examples])

    def fit(rows, labels):
        return GradientBoostingClassifier(n_estimators=n_estimators, max_depth=max_depth, learning_rate=learning_rate, subsample=subsample, random_state=seed).fit(rows, labels)
    oof = np.zeros(n)
    for train_rows, test_rows in GroupKFold(min(folds, len(set(groups.tolist())))).split(X, y, groups):
        oof[test_rows] = fit(X[train_rows], y[train_rows]).predict_proba(X[test_rows])[:, 1]
    model = fit(X, y)

    audited = [(float(oof[i]), bool(y[i])) for i, e in enumerate(examples) if e["source"] == "audit"]
    in_review = lambda e: e["features"]["score"] < AUTO_MERGE_SCORE            # the rule score sent this pair to review
    # POLICY B. The rule score's automatic merges stay. The model promotes review pairs at the lowest probability that keeps the POOLED estimated false rate of ALL
    # automatic merges within target * margin: judged against what the rule score's own merges were audited to be. A point estimate, guarded by the audits that keep
    # running and by the rollback (app/monitor.py), not a certificate.
    rule_audited = [bool(y[i]) for i, e in enumerate(examples) if e["source"] == "audit" and not in_review(e)]
    rule_false = sum(1 for ok in rule_audited if not ok)
    labeled_review = [(float(oof[i]), bool(y[i])) for i, e in enumerate(examples) if in_review(e)]   # steward and audit labels alike
    population_p = []
    if population:
        population_p = [float(v) for v in model.predict_proba(vec.transform(population).astype(np.float32).astype(np.float64))[:, 1]]
    proposed, selection = ls.select_promotion_threshold(labeled_review, population_p, n_rule_auto, rule_false, len(rule_audited), target, margin)
    policy = {"mode": "monitored", "target_false_rate": target, "confidence": confidence, "margin": margin,
              "promote_threshold": proposed if activate else None,            # LIVE: what the engines act on
              "promote_threshold_uncertified": proposed,                       # PROPOSED: what the shadow report shows; not live until the model is trained with --activate
              "selection": selection, "drop_threshold": None,
              "rule_auto_merges_audited": {"audited": len(rule_audited), "false": rule_false, "upper_bound": ls.upper_bound(rule_false, len(rule_audited), confidence) if rule_audited else None},
              "monitoring": {"audit_rate": ls.audit_rate(), "min_promoted_audits": 100, "rollback": "automatic: python3 domains_cli.py scorer check, on a schedule"}}

    body = export_body(model, list(vec.get_feature_names_out()), learning_rate)
    doc = {**body, "model_id": ls.model_id_of(body), "policy": policy,
           "trained": {"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "labels": n, "same": int(y.sum()), "different": int(n - y.sum()),
                       "audit_labels": len(audited), "steward_labels": n - len(audited), "audited_review_band": sum(1 for e in examples if e["source"] == "audit" and in_review(e)), "n_trees": len(body["trees"]), "max_depth": max_depth}}

    # the export must reproduce scikit-learn exactly, or nothing is written
    scorer = ls.TreeScorer(doc)
    raw = model.decision_function(X)
    sample = range(0, n, max(1, n // 4000))
    worst = max(abs(scorer.raw(examples[i]["features"]) - float(raw[i])) for i in sample)
    if worst > 1e-9:
        raise ExportMismatch(f"the exported trees differ from scikit-learn's by {worst:g}; refusing to write the model")
    started = time.perf_counter()
    for i in sample:
        scorer.probability(examples[i]["features"])
    per_pair_us = (time.perf_counter() - started) / max(1, len(sample)) * 1e6
    from sklearn.metrics import roc_auc_score
    report = {"labels": n, "same": int(y.sum()), "audit_labels": len(audited), "out_of_fold_auc": float(roc_auc_score(y, oof)), "worst_export_difference": worst,
              "microseconds_per_pair": per_pair_us, "oof": oof, "proposed": proposed, "selection": selection}
    return doc, report


# --------------------------------------------------------------------------- inputs
def examples_from_store(store, tenant_id: str, domain: str) -> list:
    labeled = store.human_labeled_candidates(tenant_id, domain)
    parent: dict = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for cand, _label, _source in labeled:  # records joined by a labeled pair share a group
        parent[find(cand.record_a_id)] = find(cand.record_b_id)
    return [{"features": ls.evidence_features(c.score, c.match_type, c.contributions), "label": label, "source": source, "group": find(c.record_a_id)}
            for c, label, source in labeled]


def population_from_store(store, tenant_id: str, domain: str) -> tuple:
    """(the evidence of every PENDING review pair, how many automatic merges the rule score has made): what the pooled estimate is weighted by."""
    pending = store.list_candidates(tenant_id, domain, status="pending")
    return [ls.evidence_features(c.score, c.match_type, c.contributions) for c in pending], store.count_auto_merges(tenant_id, domain)["rule"]


def examples_from_pairs(path: str, regime: str, audit_rate: float, seed: int, limit: int = 0, review_audit_rate: float = 0.0) -> tuple:
    """A synthetic world: every pair has the generator's truth. 'realistic' labels only what stewards and an audit would: the review band, and a random
    share of the automatic merges. 'all' labels everything (an upper bound)."""
    data = pickle.load(open(path, "rb"))
    rng = random.Random(seed)
    examples = []
    for d in data:
        auto = d["status"] == "confirmed"
        if regime == "realistic" and auto and rng.random() >= audit_rate:
            continue
        audited = auto or rng.random() < review_audit_rate  # a share of the REVIEW band is audited at random too (that is what certifies promoting from it)
        examples.append({"features": ls.evidence_features(d["score"], d["type"], d["contrib"]), "label": int(d["same"]), "source": "audit" if audited else "steward", "group": d["ident"]})
    if limit and len(examples) > limit:  # scikit-learn's boosting is slow on tens of thousands of rows; a random subset keeps a demonstration in minutes
        examples = random.Random(seed).sample(examples, limit)
    population = [ls.evidence_features(d["score"], d["type"], d["contrib"]) for d in data if d["status"] == "pending"]
    return examples, data, population, sum(1 for d in data if d["status"] == "confirmed")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tenant", default="default")
    ap.add_argument("--domain", default="customer")
    ap.add_argument("--db", default=None, help="SQLite path or postgresql:// URL (default: DATABASE_URL, else mdm.db)")
    ap.add_argument("--target", type=float, default=0.0005, help="the false-merge rate automatic merging must stay within (default 0.0005 = 1 in 2,000)")
    ap.add_argument("--confidence", type=float, default=0.95)
    ap.add_argument("--min-labels", type=int, default=200)
    ap.add_argument("--out", default=None, help="where to write the model (default: models/<tenant>__<domain>_scorer.json)")
    ap.add_argument("--pairs", default=None, help="train on a synthetic pairs pickle instead of the database")
    ap.add_argument("--regime", choices=("realistic", "all"), default="realistic")
    ap.add_argument("--audit-rate", type=float, default=0.02)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--folds", type=int, default=5, help="cross-validation folds for the out-of-fold predictions")
    ap.add_argument("--trees", type=int, default=150)
    ap.add_argument("--margin", type=float, default=1.0, help="keep the pooled estimate within target x margin (1.0 = the target itself; the audits and the rollback are the guard, not the margin)")
    ap.add_argument("--activate", action="store_true", help="make the promotion threshold LIVE (default: only PROPOSE it; look at the shadow report first)")
    ap.add_argument("--review-audit-rate", type=float, default=0.0, help="with --pairs: also audit this random share of the review band")
    ap.add_argument("--limit", type=int, default=0, help="with --pairs: train on a random subset of this many labelled pairs")
    args = ap.parse_args()
    data = None
    if args.pairs:
        examples, data, population, n_rule = examples_from_pairs(args.pairs, args.regime, args.audit_rate, args.seed, args.limit, args.review_audit_rate)
        out = args.out or "scorer.json"
    else:
        from app.domain_config import load_configured_domains
        from app.store_factory import get_store
        load_configured_domains()
        store = get_store(args.db)
        examples = examples_from_store(store, args.tenant, args.domain)
        population, n_rule = population_from_store(store, args.tenant, args.domain)
        out = args.out or str(ls.model_path(args.tenant, args.domain))
    try:
        doc, report = train(examples, target=args.target, confidence=args.confidence, min_labels=args.min_labels, seed=args.seed, folds=args.folds, n_estimators=args.trees,
                              population=population, n_rule_auto=n_rule, margin=args.margin, activate=args.activate)
    except NotEnoughLabels as e:
        print(f"Not trained: {e}", file=sys.stderr)
        return 2
    from pathlib import Path
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(__import__("json").dumps(doc, separators=(",", ":")))
    t = doc["trained"]
    print(f"Trained on {t['labels']:,} human labels ({t['steward_labels']:,} steward decisions, {t['audit_labels']:,} audited merges; {t['same']:,} same, {t['different']:,} different).")
    print(f"  out-of-fold AUC {report['out_of_fold_auc']:.4f}; the exported trees match scikit-learn to {report['worst_export_difference']:.1e}; {report['microseconds_per_pair']:.0f} microseconds per pair in pure Python")
    policy = doc["policy"]
    today = policy["rule_auto_merges_audited"]
    if today["audited"]:
        print(f"  TODAY's automatic merges, by audit: {today['audited']:,} checked, {today['false']} wrong; 95% upper bound on their false rate {today['upper_bound']:.4%} (target {args.target:.4%}).")
    sel = policy["selection"]
    if policy["promote_threshold_uncertified"] is None:
        print(f"  NO PROMOTION THRESHOLD: {sel.get('why')}. The model behaves exactly like the rule score. (Pooled estimate with no promotion: {sel.get('pooled_with_no_promotion', float('nan')):.4%}; "
              f"limit {args.target * args.margin:.4%}.)" if "pooled_with_no_promotion" in sel else f"  NO PROMOTION THRESHOLD: {sel.get('why')}.")
    else:
        print(f"  POLICY B: promote review pairs at p >= {policy['promote_threshold_uncertified']:.4f}: {sel['promoted_population']:,} pending review pairs would become automatic merges. "
              f"Estimated false rate of those {sel['promoted_false_rate_estimate']:.3%}, of the rule's merges {sel['rule_false_rate_estimate']:.3%}; POOLED {sel['pooled_estimate']:.4%} against the limit {sel['limit']:.4%}.")
        print("  LIVE: the engines will promote from now on (and the monitor will roll it back if the audits say it is hurting)." if policy["promote_threshold"] is not None
              else "  PROPOSED ONLY, not live. Read the shadow report, then train again with --activate.")
    if data is not None:  # a synthetic world knows the truth of EVERY pair, so the policy can be checked against it
        scorer = ls.TreeScorer(doc)
        probabilities = [scorer.score_candidate(d["score"], d["type"], d["contrib"]) for d in data]
        rule = ["auto_merge" if d["score"] >= AUTO_MERGE_SCORE else "review" for d in data]
        if policy["promote_threshold_uncertified"] is not None:
            chosen = [d["same"] for d, p, r in zip(data, probabilities, rule) if scorer.decide(p, r, uncertified_ok=True) == "auto_merge"]
            wrong = sum(1 for same in chosen if not same)
            promoted = [d["same"] for d, p, r in zip(data, probabilities, rule) if r == "review" and scorer.decide(p, r, uncertified_ok=True) == "auto_merge"]
            print(f"  synthetic truth, ALL {len(data):,} pairs, proposed policy: {len(chosen):,} auto-merged, {wrong} wrong (1 in {len(chosen) / max(wrong, 1):,.0f}); the rule score alone auto-merged {rule.count('auto_merge'):,}. "
                  f"Of the {len(promoted):,} promoted, {sum(1 for s_ in promoted if not s_)} are wrong.")
    print(f"  wrote {out}  (model {doc['model_id']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
