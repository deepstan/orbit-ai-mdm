"""
Matching benchmark: labeled synthetic data with CONTROLLED error types.

Why it exists: without ground truth we can't tell an improvement from a mere
change. This builds a Customer dataset where every record carries a hidden true
identity, runs it through the real pipeline (ingest_batch, the real index, the
real scoring), and measures what came out against the truth.

The dataset is a mixture of named SCENARIOS plus a realistic background:

  positives  (records of ONE person; they should link)
    exact_dup         same data, formatting noise only
    nickname          Bob vs Robert, same address, no email/phone
    nickname_with_email  Bob vs Robert, same address AND the same email and phone
    typo_early        surname typo inside the first 3 letters (Smith/Smyth)
    typo_late         surname typo after the 3rd letter
    moved_typo_phone  surname typo + the person moved + phone digits transposed
    missing_fields    second record has only name + zip
    email_changed     same person, new email (phone/address same)
    phone_changed     same person, new phone (email/address same)

  hard negatives  (records of DIFFERENT people; they must NOT merge)
    household         spouses: same surname/address/phone/email, different first names
    jr_sr             father and son at one address ("Park Sr" / "Park Jr")
    placeholder       strangers sharing a placeholder email/phone
    same_name         two different people, same common name, same city and zip

  background: single-record people drawn from a Zipf-like name distribution, so
  "John Smith" is common and "Zbigniew Kowalczyk" is rare, as in real data. It
  also creates realistic bucket pressure.

What the numbers mean, honestly:
  * Synthetic data flatters whatever it was designed to include. The scenarios
    here were written to cover defects found by inspection, so improvements on
    them are expected BY CONSTRUCTION. The informative signals are (a) nothing
    regressed on the scenarios that already worked, (b) the cost: review-queue
    size and candidate-set size, and (c) results on SEEDS NOT USED WHILE TUNING.
  * Typos are a mix: 60% vowel-for-vowel (phonetically neutral), 40% random letter.
    That mix decides how much a phonetic key can help, so it is stated, not hidden.
  * Real steward decisions are the true ground truth. This is a stand-in until
    enough of them exist.
"""
from __future__ import annotations

import json
import os
import random
import tempfile
import time
from collections import Counter, defaultdict
from itertools import combinations

from .db import Store
from dataclasses import dataclass

from .domains import get_domain_config
from .matching import blocking_keys
from .pipeline import ingest_batch
from .standardize import standardize

DOMAIN = "customer"
TENANT = "bench"

POSITIVE = ["exact_dup", "nickname", "nickname_with_email", "typo_early", "typo_late", "moved_typo_phone",
            "missing_fields", "email_changed", "phone_changed"]
NEGATIVE = ["household", "jr_sr", "placeholder", "same_name"]

FIRST_COMMON = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
                "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
                "Daniel", "Nancy", "Matthew", "Lisa", "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra",
                "Steven", "Ashley", "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna", "Kevin", "Michelle"]
FIRST_RARE = ["Zbigniew", "Oluwaseun", "Saoirse", "Anneliese", "Thaddeus", "Priyanka", "Casimir", "Ximena",
              "Bartholomew", "Ingrid", "Lakshmi", "Dmitri", "Yusuf", "Marisol", "Tadeusz", "Fionnuala"]
LAST_COMMON = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
               "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
               "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"]
LAST_RARE = ["Kowalczyk", "Nakamura", "Oyelaran", "Vandenberg", "Papadopoulos", "Okonkwo", "Lindqvist", "Abernathy",
             "Castellanos", "Szymanski", "Yamashita", "Brzezinski", "Duquesne", "Halvorsen", "Tsvetkov", "Marchetti"]
NICKNAMES = {"Robert": "Bob", "William": "Bill", "James": "Jim", "Michael": "Mike", "Elizabeth": "Liz",
             "Margaret": "Peggy", "John": "Jack", "Richard": "Dick", "Thomas": "Tom", "Charles": "Chuck",
             "Joseph": "Joe", "Jennifer": "Jen", "Patricia": "Trish", "Kimberly": "Kim"}
CITIES = [("San Francisco", "CA", "94107"), ("Chicago", "IL", "60601"), ("Austin", "TX", "73301"),
          ("Boston", "MA", "02108"), ("Denver", "CO", "80202"), ("Seattle", "WA", "98101"), ("Miami", "FL", "33101"),
          ("Portland", "OR", "97201"), ("Phoenix", "AZ", "85001"), ("Reno", "NV", "89501"), ("Dallas", "TX", "75201"),
          ("Atlanta", "GA", "30301"), ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"),
          ("Houston", "TX", "77001"), ("Philadelphia", "PA", "19101"), ("San Diego", "CA", "92101"),
          ("Columbus", "OH", "43201"), ("Nashville", "TN", "37201"), ("Detroit", "MI", "48201")]
STREETS = ["Main St", "Oak Ave", "Elm St", "Maple Dr", "Birch Ln", "Pine Rd", "Cedar Blvd", "Washington St",
           "Lake Shore Dr", "Park Ave", "Sunset Blvd", "River Rd", "Highland Ave", "Church St", "School St",
           "Mill Rd", "Hill St", "Spring St", "Walnut Ave", "Chestnut St"]
MAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "workmail.com"]
PLACEHOLDERS = [("email", "none@none.com"), ("email", "noemail@noemail.com"), ("email", "unknown"),
                ("phone", "000-000-0000"), ("phone", "1234567890"), ("phone", "999-999-9999")]
VOWELS = "aeiou"


# --------------------------------------------------------------------- building blocks
def _pick(rng, common, rare, rare_share=0.15):
    if rng.random() < rare_share:
        return rng.choice(rare)
    weights = [1.0 / (i + 1) for i in range(len(common))]  # Zipf-like: rank 1 is far more common than rank 40
    return rng.choices(common, weights=weights, k=1)[0]


def make_person(rng, **over):
    first, last = _pick(rng, FIRST_COMMON, FIRST_RARE), _pick(rng, LAST_COMMON, LAST_RARE)
    city, state, zip5 = rng.choice(CITIES)
    p = {"first_name": first, "last_name": last, "address_line1": f"{rng.randint(1, 9999)} {rng.choice(STREETS)}",
         "city": city, "state": state, "postal_code": zip5,
         "phone": f"{rng.randint(201, 989)}-{rng.randint(201, 989)}-{rng.randint(1000, 9999)}",
         "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@{rng.choice(MAIL_DOMAINS)}"}
    p.update(over)
    return p


def rec(p, **over):
    keys = ("first_name", "last_name", "email", "phone", "address_line1", "city", "state", "postal_code")
    out = {k: p.get(k, "") for k in keys}
    out.update(over)
    return out


def typo(rng, word, early):
    """One substitution. early=True: inside the first 3 letters (positions 1-2, so the initial is kept)."""
    if len(word) < 5:
        early = True
    i = rng.choice([1, 2]) if early else rng.randint(3, len(word) - 1)
    ch = word[i].lower()
    if rng.random() < 0.6 and ch in VOWELS:  # phonetically neutral vowel swap
        new = rng.choice([v for v in VOWELS if v != ch])
    else:
        new = rng.choice([c for c in "abcdefghijklmnopqrstuvwxyz" if c != ch])
    return word[:i] + (new.upper() if word[i].isupper() else new) + word[i + 1:]


def transpose_digits(rng, phone):
    digits = [c for c in phone if c.isdigit()]
    for _ in range(20):
        i = rng.randint(3, len(digits) - 2)
        if digits[i] != digits[i + 1]:
            digits[i], digits[i + 1] = digits[i + 1], digits[i]
            break
    return "".join(digits[:3]) + "-" + "".join(digits[3:6]) + "-" + "".join(digits[6:])


def noisy(rng, r):
    """Formatting noise that standardization should erase completely."""
    r = dict(r)
    r["last_name"] = rng.choice([r["last_name"].upper(), r["last_name"].lower(), r["last_name"]])
    r["address_line1"] = r["address_line1"].replace(" St", " Street").replace(" Ave", " Avenue").replace(" Dr", " Drive")
    if r["phone"]:
        d = "".join(c for c in r["phone"] if c.isdigit())
        r["phone"] = rng.choice([f"({d[:3]}) {d[3:6]}-{d[6:]}", f"{d[:3]}.{d[3:6]}.{d[6:]}", f"+1 {d[:3]} {d[3:6]} {d[6:]}"])
    if r["email"]:
        r["email"] = " " + r["email"].upper() + " "
    return r


# ------------------------------------------------------------------------- scenarios
def _case(scenario, rng):
    """Returns [(raw_record, person_index)], where records with the same index are the same person."""
    if scenario == "exact_dup":
        p = make_person(rng)
        return [(rec(p), 0), (noisy(rng, rec(p)), 0)]
    if scenario == "nickname":
        p = make_person(rng, first_name=rng.choice(list(NICKNAMES)))
        return [(rec(p, email="", phone=""), 0), (rec(p, first_name=NICKNAMES[p["first_name"]], email="", phone=""), 0)]
    if scenario == "nickname_with_email":
        p = make_person(rng, first_name=rng.choice(list(NICKNAMES)))
        return [(rec(p), 0), (rec(p, first_name=NICKNAMES[p["first_name"]]), 0)]
    if scenario in ("typo_early", "typo_late"):
        p = make_person(rng)
        return [(rec(p, email="", phone=""),0), (rec(p, last_name=typo(rng, p["last_name"], scenario == "typo_early"), email="", phone=""), 0)]
    if scenario == "moved_typo_phone":
        p = make_person(rng)
        city, state, zip5 = rng.choice([c for c in CITIES if c[2] != p["postal_code"]])
        moved = {"address_line1": f"{rng.randint(1, 9999)} {rng.choice(STREETS)}", "city": city, "state": state, "postal_code": zip5}
        return [(rec(p, email=""), 0),
                (rec(p, last_name=typo(rng, p["last_name"], True), phone=transpose_digits(rng, p["phone"]), email="", **moved), 0)]
    if scenario == "missing_fields":
        p = make_person(rng)
        return [(rec(p), 0), (rec(p, email="", phone="", address_line1="", city="", state=""), 0)]
    if scenario == "email_changed":
        p = make_person(rng)
        return [(rec(p), 0), (rec(p, email=f"{p['first_name'].lower()}{rng.randint(1000, 9999)}@{rng.choice(MAIL_DOMAINS)}"), 0)]
    if scenario == "phone_changed":
        p = make_person(rng)
        return [(rec(p), 0), (rec(p, phone=f"{rng.randint(201, 989)}-{rng.randint(201, 989)}-{rng.randint(1000, 9999)}"), 0)]
    if scenario == "household":
        a = make_person(rng)
        b_first = rng.choice([n for n in FIRST_COMMON if n[0] != a["first_name"][0]])
        b = {**a, "first_name": b_first}  # same surname, address, phone and the family email
        return [(rec(a), 0), (rec(b), 1)]
    if scenario == "jr_sr":
        p = make_person(rng)
        shared_phone = rng.random() < 0.5
        a = rec(p, last_name=f"{p['last_name']} Sr")
        b = rec(p, last_name=f"{p['last_name']} Jr", email=f"{p['first_name'].lower()}.jr{rng.randint(1, 999)}@{rng.choice(MAIL_DOMAINS)}",
                phone=p["phone"] if shared_phone else f"{rng.randint(201, 989)}-{rng.randint(201, 989)}-{rng.randint(1000, 9999)}")
        return [(a, 0), (b, 1)]
    if scenario == "placeholder":
        kind, value = rng.choice(PLACEHOLDERS)
        return [(rec(make_person(rng), **{kind: value}), 0), (rec(make_person(rng), **{kind: value}), 1)]
    if scenario == "same_name":
        a = make_person(rng, first_name=rng.choice(FIRST_COMMON[:10]), last_name=rng.choice(LAST_COMMON[:10]))
        b = make_person(rng, first_name=a["first_name"], last_name=a["last_name"], city=a["city"], state=a["state"], postal_code=a["postal_code"])
        return [(rec(a), 0), (rec(b), 1)]
    raise ValueError(scenario)


def generate(seed: int, scale: int = 25):
    """scale = cases per scenario; background = 10 x scale single-record people."""
    rng = random.Random(seed)
    rows, true_id = [], 0
    for scenario in POSITIVE + NEGATIVE:
        for case in range(scale):
            local = {}
            for raw, who in _case(scenario, rng):
                local.setdefault(who, true_id + len(local))
                rows.append({"raw": raw, "true": local[who], "scenario": scenario, "case": f"{scenario}:{case}"})
            true_id += len(local)
    for i in range(scale * 10):
        rows.append({"raw": rec(make_person(rng)), "true": true_id, "scenario": "background", "case": f"background:{i}"})
        true_id += 1
    rng.shuffle(rows)
    return rows


# --------------------------------------------------------------------------- running
def _pairs(n):
    return n * (n - 1) // 2


@dataclass
class Profile:
    domain: str
    positive: list
    negative: list
    generate: object


def get_profile(name: str = "customer", variant: str | None = None) -> Profile:
    """"customer": the original benchmark. "person": SSN + date of birth (app/benchmark_person.py); `variant` picks which
    blocking keys are live (see benchmark_person.VARIANTS)."""
    if name == "customer":
        return Profile(DOMAIN, POSITIVE, NEGATIVE, generate)
    if name == "person":
        from . import benchmark_person as bp
        bp.register_person(variant or "full")
        return Profile(bp.DOMAIN, bp.POSITIVE, bp.NEGATIVE, bp.generate)
    raise ValueError(f"unknown profile '{name}' (customer, person)")


def _names(agg: dict):
    prof = get_profile(agg.get("profile", "customer"), agg.get("variant"))
    return prof.positive, prof.negative


def run_one(seed: int, scale: int = 25, domain_label: str = "", profile: str = "customer", variant: str | None = None) -> dict:
    prof = get_profile(profile, variant)
    domain = prof.domain
    rows = prof.generate(seed, scale)
    cfg = get_domain_config(domain)
    tmp = tempfile.mkdtemp()
    store = Store(os.path.join(tmp, "bench.db"))

    started = time.time()
    results = []
    for i in range(0, len(rows), 200):
        chunk = rows[i:i + 200]
        results += ingest_batch(store, TENANT, domain, [("bench", f"r{i + j}", r["raw"]) for j, r in enumerate(chunk)])
    seconds = time.time() - started

    rid = [r["record_id"] for r in results]
    entity_of = {r.id: (r.entity_id or f"solo:{r.id}") for r in store.list_source_records(TENANT, domain)}
    pending = {frozenset((c.record_a_id, c.record_b_id)) for c in store.list_candidates(TENANT, domain, "pending")}
    ent = [entity_of[x] for x in rid]
    n = len(rows)

    # ---- overall pairwise, on auto-linked clusters
    true_pairs = sum(_pairs(c) for c in Counter(r["true"] for r in rows).values())
    pred_pairs = sum(_pairs(c) for c in Counter(ent).values())
    tp = sum(_pairs(c) for c in Counter(zip(ent, (r["true"] for r in rows))).values())
    pending_true = sum(1 for p in pending if len({rows[rid.index(x)]["true"] for x in p}) == 1) if pending else 0

    # ---- blocking: which pairs share a usable key (computed from the live config)
    cap = getattr(cfg, "max_bucket_size", None) or 10 ** 9
    keys_of = [blocking_keys(standardize(domain, r["raw"]), cfg) for r in rows]
    bucket = defaultdict(list)
    for i, ks in enumerate(keys_of):
        for k in ks:
            bucket[k].append(i)
    cand = set()
    for members in bucket.values():
        if len(members) <= cap:
            cand.update(combinations(members, 2))
    sizes = sorted(len(m) for m in bucket.values())

    # ---- per scenario
    by_case = defaultdict(list)
    for i, r in enumerate(rows):
        by_case[r["case"]].append(i)
    scenarios = defaultdict(lambda: Counter())
    for case, idxs in by_case.items():
        scn = rows[idxs[0]]["scenario"]
        if scn == "background":
            continue
        for a, b in combinations(idxs, 2):
            same = rows[a]["true"] == rows[b]["true"]
            c = scenarios[scn]
            c["pairs"] += 1
            c["merged"] += ent[a] == ent[b]
            c["queued"] += frozenset((rid[a], rid[b])) in pending
            c["shares_key"] += (min(a, b), max(a, b)) in cand
            c["is_positive"] = int(same)

    return {
        "seed": seed, "scale": scale, "label": domain_label, "profile": profile, "variant": variant, "records": n, "seconds": round(seconds, 2),
        "overall": {
            "true_pairs": true_pairs, "pred_pairs": pred_pairs, "tp": tp, "fp": pred_pairs - tp,
            "pending": len(pending), "pending_true": pending_true,
            "candidate_pairs": len(cand), "all_pairs": _pairs(n),
            "bucket_max": sizes[-1] if sizes else 0, "bucket_p99": sizes[int(len(sizes) * 0.99)] if sizes else 0,
        },
        "scenarios": {k: dict(v) for k, v in scenarios.items()},
    }


def aggregate(runs: list) -> dict:
    """Pools COUNTS across seeds (then derives rates), which is the right way to combine them."""
    out = {"label": runs[0]["label"], "profile": runs[0].get("profile", "customer"), "variant": runs[0].get("variant"), "seeds": [r["seed"] for r in runs], "scale": runs[0]["scale"],
           "records": sum(r["records"] for r in runs), "seconds": round(sum(r["seconds"] for r in runs), 2),
           "overall": Counter(), "scenarios": defaultdict(Counter)}
    for r in runs:
        for k, v in r["overall"].items():
            out["overall"][k] = max(out["overall"][k], v) if k.startswith("bucket") else out["overall"][k] + v
        for s, c in r["scenarios"].items():
            for k, v in c.items():
                out["scenarios"][s][k] = v if k == "is_positive" else out["scenarios"][s][k] + v
    out["overall"] = dict(out["overall"])
    out["scenarios"] = {k: dict(v) for k, v in out["scenarios"].items()}
    out["records_per_sec"] = round(out["records"] / out["seconds"], 1) if out["seconds"] else 0
    return out


def _rate(a, b):
    return a / b if b else 0.0


def summarize(agg: dict) -> dict:
    o = agg["overall"]
    pos = {s: c for s, c in agg["scenarios"].items() if c.get("is_positive")}
    neg = {s: c for s, c in agg["scenarios"].items() if not c.get("is_positive")}
    pp, pm, pq, pk = (sum(c.get(k, 0) for c in pos.values()) for k in ("pairs", "merged", "queued", "shares_key"))
    npairs, nm, nq = (sum(c.get(k, 0) for c in neg.values()) for k in ("pairs", "merged", "queued"))
    return {
        "records": agg["records"], "records_per_sec": agg["records_per_sec"],
        "auto_precision": _rate(o["tp"], o["pred_pairs"]), "auto_recall": _rate(o["tp"], o["true_pairs"]),
        "false_merged_pairs": o["fp"], "review_pairs": o["pending"], "review_precision": _rate(o["pending_true"], o["pending"]),
        "pos_auto_recall": _rate(pm, pp), "pos_recall_with_review": _rate(pm + pq, pp), "pos_blocking_recall": _rate(pk, pp),
        "neg_wrong_merge_rate": _rate(nm, npairs), "neg_queued_rate": _rate(nq, npairs),
        "reduction_ratio": 1 - _rate(o["candidate_pairs"], o["all_pairs"]),
        "avg_candidates_per_record": _rate(2 * o["candidate_pairs"], agg["records"]),
        "bucket_max": o["bucket_max"], "bucket_p99": o["bucket_p99"],
    }


def run(seeds, scale=25, label="", profile="customer", variant=None) -> dict:
    runs = [run_one(s, scale, label, profile, variant) for s in seeds]
    agg = aggregate(runs)
    agg["summary"] = summarize(agg)
    return agg


def format_report(agg: dict) -> str:
    s, lines = agg["summary"], []
    POSITIVE_, NEGATIVE_ = _names(agg)
    lines.append(f"Benchmark '{agg['label']}'  profile={agg.get('profile', 'customer')}{' / ' + agg['variant'] if agg.get('variant') else ''}  seeds={agg['seeds']}  scale={agg['scale']}  records={s['records']}  "
                 f"({s['records_per_sec']} rec/s)")
    lines.append("")
    lines.append(f"{'POSITIVES (should link)':26s} {'auto-linked':>12s} {'+review':>9s} {'shares a key':>13s}")
    for name in POSITIVE_:
        c = agg["scenarios"].get(name)
        if c:
            lines.append(f"  {name:24s} {_rate(c['merged'], c['pairs']):12.0%} {_rate(c['merged'] + c['queued'], c['pairs']):9.0%} "
                         f"{_rate(c['shares_key'], c['pairs']):13.0%}")
    lines.append(f"  {'ALL POSITIVES':24s} {s['pos_auto_recall']:12.0%} {s['pos_recall_with_review']:9.0%} {s['pos_blocking_recall']:13.0%}")
    lines.append("")
    lines.append(f"{'HARD NEGATIVES (must not merge)':32s} {'WRONG MERGES':>13s} {'sent to review':>15s}")
    for name in NEGATIVE_:
        c = agg["scenarios"].get(name)
        if c:
            lines.append(f"  {name:30s} {c['merged']:>5d}/{c['pairs']:<7d} {c['queued']:>7d}/{c['pairs']:<7d}")
    lines.append(f"  {'ALL NEGATIVES':30s} {s['neg_wrong_merge_rate']:12.0%} {s['neg_queued_rate']:15.0%}")
    lines.append("")
    lines.append(f"Overall auto-link:  precision {s['auto_precision']:.1%}   recall {s['auto_recall']:.1%}   false-merged pairs {s['false_merged_pairs']}")
    lines.append(f"Review queue:       {s['review_pairs']} pairs, {s['review_precision']:.0%} are true duplicates")
    lines.append(f"Blocking:           reduction ratio {s['reduction_ratio']:.2%}, ~{s['avg_candidates_per_record']:.1f} candidates/record, "
                 f"largest bucket {s['bucket_max']} (p99 {s['bucket_p99']})")
    return "\n".join(lines)


def compare(before: dict, after: dict) -> str:
    a, b = before["summary"], after["summary"]
    rows = [("auto-link precision", "auto_precision", "%"), ("auto-link recall", "auto_recall", "%"),
            ("false-merged pairs", "false_merged_pairs", "n"), ("positives: auto-linked", "pos_auto_recall", "%"),
            ("positives: linked or reviewed", "pos_recall_with_review", "%"), ("positives: blocking recall", "pos_blocking_recall", "%"),
            ("negatives: WRONG merges", "neg_wrong_merge_rate", "%"), ("negatives: sent to review", "neg_queued_rate", "%"),
            ("review-queue pairs", "review_pairs", "n"), ("review-queue precision", "review_precision", "%"),
            ("candidates per record", "avg_candidates_per_record", "f"), ("largest bucket", "bucket_max", "n"),
            ("records / second", "records_per_sec", "f")]
    out = [f"{'':32s} {before['label'][:14]:>14s} {after['label'][:14]:>14s}"]
    for label, key, kind in rows:
        fmt = {"%": lambda v: f"{v:.1%}", "n": lambda v: f"{v:,.0f}", "f": lambda v: f"{v:,.1f}"}[kind]
        out.append(f"{label:32s} {fmt(a[key]):>14s} {fmt(b[key]):>14s}")
    out.append("")
    out.append(f"{'per scenario (pairs linked)':32s} {'before':>14s} {'after':>14s}")
    pos_, neg_ = _names(before)
    for name in pos_ + neg_:
        ca, cb = before["scenarios"].get(name), after["scenarios"].get(name)
        if ca and cb:
            out.append(f"{name:32s} {_rate(ca['merged'], ca['pairs']):14.0%} {_rate(cb['merged'], cb['pairs']):14.0%}"
                       f"   {'(positive: higher is better)' if ca.get('is_positive') else '(negative: lower is better)'}")
    return "\n".join(out)


def save(agg: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True)


def load(path: str) -> dict:
    with open(path) as f:
        return json.load(f)
