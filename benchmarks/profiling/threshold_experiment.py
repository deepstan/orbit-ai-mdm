"""
Bounding experiment for learned thresholds: how many pairs could be auto-merged, and how many wrongly queued for review could be dropped,
while auto-merges stay within a target false-merge rate? Input: /tmp/pairs.pkl (every scored pair of a store with the generator's truth).
Uses scikit-learn, which the project itself does not depend on.   python threshold_experiment.py [target_false_rate=0.0005]
"""
import json, pickle, sys, time
import numpy as np
from scipy.stats import beta
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import GroupKFold

TARGET = float(sys.argv[1]) if len(sys.argv) > 1 else 0.0005
data = pickle.load(open("/tmp/pairs.pkl", "rb"))
T = time.time()
def feats(d):
    f = {"score": d["score"], "type:" + str(d["type"]): 1}
    for c in d["contrib"]:
        n = c["field"]
        f[f"{n}:{c.get('state')}"] = 1
        f[f"{n}:sim"] = c.get("similarity") if c.get("similarity") is not None else -1
        f[f"{n}:pts"] = c.get("points", 0)
    return f
X = DictVectorizer(sparse=False).fit_transform([feats(d) for d in data])
y = np.array([d["same"] for d in data], dtype=int)
score = np.array([d["score"] for d in data])
groups = np.array([hash(d["ident"]) % 10_000_019 for d in data])
is_auto = np.array([d["status"] == "confirmed" for d in data])
print(f"{len(y):,} pairs, {X.shape[1]} features, {y.sum():,} real duplicates; features built in {time.time() - T:.0f}s", flush=True)

def upper(fp, n):  # one-sided 95% Clopper-Pearson upper bound on the false rate
    return 1.0 if n == 0 else (beta.ppf(0.95, fp + 1, n - fp) if fp < n else 1.0)

def best_auto(p):
    """the largest set of top-ranked pairs whose false rate stays within the target: by point estimate, and by the 95% upper bound"""
    order = np.argsort(-p, kind="stable"); fp = np.cumsum(1 - y[order]); n = np.arange(1, len(p) + 1)
    ok_point = np.where(fp / n <= TARGET)[0]
    k_point = ok_point[-1] + 1 if len(ok_point) else 0
    k_safe = 0
    for k in range(max(k_point, 1), 0, -max(1, k_point // 400)):   # walk down in steps until the upper bound fits
        if upper(fp[k - 1], k) <= TARGET:
            k_safe = k; break
    return order, k_point, k_safe, fp

def oof(train_mask_fn):
    p = np.zeros(len(y))
    for tr, te in GroupKFold(5).split(X, y, groups):
        tr = tr[train_mask_fn(tr)]
        m = HistGradientBoostingClassifier(max_iter=150, learning_rate=0.1, random_state=0).fit(X[tr], y[tr])
        p[te] = m.predict_proba(X[te])[:, 1]
    return p
rng = np.random.default_rng(0)
audit = rng.random(len(y)) < 0.02      # a 2% random audit of auto-merged pairs
results = {}
def report(name, p):
    order, kp, ks, fp = best_auto(p)
    n_true = int(y.sum())
    auto_set = np.zeros(len(y), bool); auto_set[order[:kp]] = True
    lo_order = np.argsort(p)
    # auto-reject: drop the lowest-ranked pairs while losing at most 0.5% of the real duplicates
    lost = np.cumsum(y[lo_order]); n_rej = int(np.searchsorted(lost, 0.005 * n_true, side="right"))
    rejected = np.zeros(len(y), bool); rejected[lo_order[:n_rej]] = True
    queue = ~auto_set & ~rejected
    results[name] = {"auto_point": int(kp), "auto_false_point": int(fp[kp - 1]) if kp else 0, "auto_safe": int(ks), "true_dups_auto": int(y[auto_set].sum()),
                     "review_left": int(queue.sum()), "review_precision": round(float(y[queue].mean()), 3) if queue.any() else None, "auto_rejected": n_rej}
    r = results[name]
    print(f"  {name:48s} auto-merge {r['auto_point']:>7,} ({r['auto_false_point']} false, 95% bound safe up to {r['auto_safe']:,}) | real dups auto-merged {r['true_dups_auto']:,} | "
          f"review queue {r['review_left']:,} at {r['review_precision']:.1%} | dropped {r['auto_rejected']:,}", flush=True)
print(f"TARGET: at most {TARGET:.4%} false auto-merges (1 in {1 / TARGET:,.0f}).  Real duplicates among the candidate pairs: {int(y.sum()):,}")
report("today: the rule score, best cutoff", score)
print("  (today's fixed cutoff 0.92: auto-merge", int(is_auto.sum()), f"pairs, {int(((1 - y) & is_auto).sum())} false; review queue {int((~is_auto).sum()):,} at {y[~is_auto].mean():.1%})")
report("model, trained on ALL labelled pairs (upper bound)", oof(lambda tr: np.ones(len(tr), bool)))
report("model, trained on review band + 2% audit (realistic)", oof(lambda tr: (~is_auto[tr]) | audit[tr]))
json.dump(results, open("/tmp/threshold_experiment.json", "w"), indent=1)
print(f"done in {time.time() - T:.0f}s")
