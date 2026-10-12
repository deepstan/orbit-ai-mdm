"""
Does a model and its thresholds, learned on one tenant's data, hold on another's? Trains on world A (/tmp/pairs.pkl), takes the thresholds that give the target false-merge
rate on A, and applies them unchanged to world B (/tmp/pairs_shift.pkl); then compares with a model trained on B's own labels.   python transfer_experiment.py [target=0.0005]
"""
import pickle, sys
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import GroupKFold
TARGET = float(sys.argv[1]) if len(sys.argv) > 1 else 0.0005
def feats(d):
    f = {"score": d["score"], "type:" + str(d["type"]): 1}
    for c in d["contrib"]:
        n = c["field"]; f[f"{n}:{c.get('state')}"] = 1; f[f"{n}:sim"] = c.get("similarity") if c.get("similarity") is not None else -1; f[f"{n}:pts"] = c.get("points", 0)
    return f
def load(path):
    data = pickle.load(open(path, "rb"))
    return data, np.array([d["same"] for d in data], int), np.array([d["status"] == "confirmed" for d in data]), np.array([hash(d["ident"]) % 10_000_019 for d in data])
A, yA, autoA, gA = load("/tmp/pairs.pkl"); B, yB, autoB, gB = load("/tmp/pairs_shift.pkl")
vecA = DictVectorizer(sparse=False).fit([feats(d) for d in A]); XA = vecA.transform([feats(d) for d in A]); XB_on_A = vecA.transform([feats(d) for d in B])
rng = np.random.default_rng(0)
def thresholds(p, y):
    """the probability cutoffs: auto-merge above t_auto (false rate <= target), and drop below t_low (losing <= 0.5% of the real duplicates)"""
    order = np.argsort(-p, kind="stable"); fp = np.cumsum(1 - y[order]); n = np.arange(1, len(p) + 1)
    ok = np.where(fp / n <= TARGET)[0]; k = ok[-1] + 1
    lo = np.argsort(p); lost = np.cumsum(y[lo]); r = int(np.searchsorted(lost, 0.005 * y.sum(), side="right"))
    return p[order[k - 1]], p[lo[r]] if r < len(p) else 1.0
def outcome(p, y, t_auto, t_low, label):
    auto = p >= t_auto; rej = p < t_low; q = ~auto & ~rej
    fp = int(((1 - y) & auto).sum()); n = int(auto.sum())
    print(f"  {label:58s} auto-merge {n:>7,}  false {fp:>3} = 1 in {n / max(fp, 1):>7,.0f}{' (none)' if fp == 0 else ''} | real dups lost to the drop {int((y & rej).sum()):>5,} ({(y & rej).sum() / y.sum():.2%}) | review queue {int(q.sum()):>6,} at {y[q].mean() if q.any() else 0:.1%}")
# --- world A: out-of-fold predictions of the realistic regime (review band + 2% audit), to set the thresholds
audit = rng.random(len(yA)) < 0.02; pA = np.zeros(len(yA))
for tr, te in GroupKFold(5).split(XA, yA, gA):
    tr = tr[(~autoA[tr]) | audit[tr]]
    pA[te] = HistGradientBoostingClassifier(max_iter=150, random_state=0).fit(XA[tr], yA[tr]).predict_proba(XA[te])[:, 1]
t_auto, t_low = thresholds(pA, yA)
print(f"target: 1 false auto-merge in {1 / TARGET:,.0f}.  Thresholds learned on world A: auto-merge at p >= {t_auto:.4f}, drop at p < {t_low:.4f}\n")
print("WORLD A (where it was learned):"); outcome(pA, yA, t_auto, t_low, "model A, its own thresholds (out of fold)")
mA = HistGradientBoostingClassifier(max_iter=150, random_state=0).fit(XA[(~autoA) | audit], yA[(~autoA) | audit])
print("\nWORLD B (fewer identifiers, more duplicates), nothing re-learned:")
print(f"  {'fixed rule today: 0.92':58s} auto-merge {int(autoB.sum()):>7,}  false {int(((1 - yB) & autoB).sum()):>3} | review queue {int((~autoB).sum()):>6,} at {yB[~autoB].mean():.1%}")
pBA = mA.predict_proba(XB_on_A)[:, 1]; outcome(pBA, yB, t_auto, t_low, "model A + A's thresholds applied unchanged")
# --- world B with its own labels (review band + 2% audit), thresholds set on B
vecB = DictVectorizer(sparse=False).fit([feats(d) for d in B]); XB = vecB.transform([feats(d) for d in B]); auditB = rng.random(len(yB)) < 0.02; pB = np.zeros(len(yB))
for tr, te in GroupKFold(5).split(XB, yB, gB):
    tr = tr[(~autoB[tr]) | auditB[tr]]
    pB[te] = HistGradientBoostingClassifier(max_iter=150, random_state=0).fit(XB[tr], yB[tr]).predict_proba(XB[te])[:, 1]
tb_auto, tb_low = thresholds(pB, yB); outcome(pB, yB, tb_auto, tb_low, "model B, trained on B's own labels, B's thresholds")
