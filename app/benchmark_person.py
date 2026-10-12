"""
Person benchmark: SSN and date of birth, and how blocking behaves as the data grows.

Two things live here.

1. SCENARIOS (the same idea as app/benchmark.py): small labelled cases run through the REAL pipeline, to measure what
   SSN and DOB buy and what their defects cost. The `person` domain (domain_templates/person.json) is a BASELINE: it does
   NOT cleanse placeholder SSNs/DOBs yet, so the negatives that use them show the damage that cleansing has to remove.

2. A SCALE HARNESS: how many other records does a record share a usable blocking key with, at 10k, 100k, 1M ... records?
   That number ("partners") decides whether 100M records fit in a time frame, because scoring cost is proportional to it.
   It never touches a database: it streams records, computes their keys, and counts bucket sizes for a SAMPLE of records.

The generator is a pure function of (seed, index): record i can be produced without producing records 0..i-1, so it can
be streamed, sampled and parallelised. Names, zips and streets are drawn from Zipf-Mandelbrot distributions with a real
head (Smith, Johnson ... from app/benchmark.py) and a long synthetic tail; app/benchmark.py's vocabularies are far too
small to say anything about 1M records.

What this does NOT tell you, honestly: the data is synthetic. Real surnames cluster phonetically (Soundex collisions),
real zip codes and phone numbers are not uniform, and real duplicates are messier. The numbers are a measuring stick to
compare designs with each other and to see GROWTH (the exponent), not a forecast for your data. Replace the generator
with a sample of your own data (see `scale --records-file`) before trusting an absolute number.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import pickle
import random
import sqlite3
import time
from datetime import date, timedelta

from .benchmark import FIRST_COMMON, FIRST_RARE, LAST_COMMON, LAST_RARE, MAIL_DOMAINS, NICKNAMES, STREETS, transpose_digits, typo
from .db import Store
from .domain_config import parse_domain_document
from .domains import register_domain
from .matching import blocking_keys
from .standardize import standardize

DOMAIN = "person"
# SYNTHETIC data only: the person domain protects SSNs, which needs a key. This default exists so the benchmark runs anywhere; it is
# not a secret, and real deployments set MDM_TOKEN_KEY from a secrets store.
os.environ.setdefault("MDM_TOKEN_KEY", "benchmark-only-key-synthetic-data-0000000000")
TEMPLATE = os.path.join(os.path.dirname(__file__), "..", "domain_templates", "person.json")

# --------------------------------------------------------------------------- blocking candidates and variants
# Every key group the experiments consider. The shipped template (domain_templates/person.json) carries only the "scalable" set; this
# list is where the others live, so the benchmark can compare designs without anyone shipping a config with phone windows in it.
SPARSE = ["ssn", "dob", "email", "phone"]  # a record with none of these is "sparse": only then do the loose name-only keys apply
_SOUNDEX_LAST = {"field": "last_name", "as": "soundex"}
CANDIDATES = [
    [{"field": "ssn", "as": "exact"}],                                                                       # 0
    ["last_name", "postal_code"],                                                                            # 1
    [{"field": "phone", "as": "exact"}],                                                                     # 2
    [{"field": "phone", "as": "windows", "size": 4}],                                                        # 3
    [{"field": "email", "as": "exact"}],                                                                     # 4
    ["last_name", "first_name"],                                                                             # 5
    [_SOUNDEX_LAST, {"field": "postal_code", "as": "prefix", "size": 3}],                                    # 6
    [_SOUNDEX_LAST, "first_name"],                                                                           # 7
    ["first_name", "postal_code"],                                                                           # 8
    [{"field": "dob", "as": "exact"}, _SOUNDEX_LAST],                                                        # 9
    [{"field": "dob", "as": "exact"}, {"field": "first_name", "as": "prefix", "size": 1}],                   # 10
    [_SOUNDEX_LAST, "first_name", {"field": "dob", "as": "prefix", "size": 4}],                              # 11
    [{"field": "phone", "as": "transpose"}],                                                                 # 12
    {"fields": ["last_name", "first_name"], "unless_any": SPARSE},                                           # 13
    {"fields": [_SOUNDEX_LAST, "first_name"], "unless_any": SPARSE},                                         # 14
    {"fields": ["first_name", "postal_code"], "unless_any": SPARSE},                                         # 15
    [{"field": "last_name", "as": "exact"}, {"field": "postal_code", "as": "exact"}],                        # 16: the WHOLE surname and the 5-digit zip
    [_SOUNDEX_LAST, {"field": "first_name", "as": "prefix", "size": 1}, {"field": "dob", "as": "exact"}],    # 17: product of cardinalities ~1e9, so it stays tiny
    [{"field": "first_name", "as": "exact"}, {"field": "postal_code", "as": "exact"}],                        # 18: rescues a mistyped surname with no identifier left
    [{"field": "last_name", "as": "exact"}, {"field": "first_name", "as": "exact"}],                         # 19: rescues a move with no identifier left
]
GROUP_LABELS = ["ssn exact", "last+zip", "phone exact", "phone windows(4)", "email exact", "last+first", "soundex(last)+zip3",
                "soundex(last)+first", "first+zip", "dob+soundex(last)", "dob+first initial", "soundex(last)+first+birth year",
                "phone + adjacent swaps", "last+first (sparse only)", "soundex(last)+first (sparse only)", "first+zip (sparse only)",
                "last (exact)+zip5", "soundex(last)+first initial+dob", "first (exact)+zip5", "last (exact)+first (exact)"]
VARIANTS = {
    # what the customer domain's keys would do on person data: no SSN, no DOB
    "customer_like": [1, 3, 4, 5, 6, 7, 8],
    # exact SSN, then fuzzy name/address, NO phone keys
    "ssn_no_phone": [0, 1, 4, 5, 6, 7, 8],
    # fuzzy name + DOB + address + phone, NO SSN key (exact phone only)
    "dob_phone_no_ssn": [1, 2, 4, 6, 9, 10, 11],
    # the same, with the 4-digit phone windows as well
    "dob_phone_windows_no_ssn": [1, 2, 3, 4, 6, 9, 10, 11],
    # STEP 4: selective keys everywhere (SSN, email, phone with adjacent swaps, DOB combinations, last+zip, soundex+zip3), and the loose
    # name-only keys ONLY for records with no SSN, DOB, email or phone. This is what domain_templates/person.json ships (same order).
    "scalable": [0, 4, 12, 1, 6, 9, 10, 11, 13, 14, 15],
    # scalable, with the three keys that grew linearly in the 1M run (last+zip and soundex+zip3 on 3-character prefixes, dob+first initial) replaced
    # by keys whose combinations of values are far more numerous than the records: whole surname + 5-digit zip, soundex+first initial+full DOB
    "scalable_tight": [0, 4, 12, 9, 11, 16, 17, 13, 14, 15],
    # scalable_tight plus the two whole-name keys that win back the duplicates it misses (no identifier left, and a mistyped surname or a move)
    "scalable_tight_plus": [0, 4, 12, 9, 11, 16, 17, 18, 19, 13, 14, 15],
    # everything
    "full": list(range(len(CANDIDATES))),
}


def _template_doc(cleansing: bool = True) -> dict:
    with open(TEMPLATE) as f:
        doc = json.load(f)
    if not cleansing:  # the state before step 3: SSN as plain digits, DOB as plain text
        doc["fields"]["ssn"] = {"normalize": ["digits"], "present": ["present_id"]}
        doc["fields"]["dob"] = {"normalize": ["text"], "type": "date", "format": "%Y-%m-%d", "present": ["present_text"]}
    return doc


def load_config(cleansing: bool = True):
    """The person domain exactly as shipped (its blocking is the scalable set). cleansing=False swaps SSN/DOB back to plain."""
    return parse_domain_document(_template_doc(cleansing), source=TEMPLATE)


def register_person(variant: str = "full", near: bool = True):
    """Registers the `person` domain with the blocking groups of one variant. Returns the config. A ':baseline' suffix
    ("full:baseline") turns SSN/DOB cleansing off."""
    name, _, flag = variant.partition(":")
    if name not in VARIANTS or flag not in ("", "baseline"):
        raise ValueError(f"unknown variant '{variant}'. Known: {', '.join(VARIANTS)} (each optionally with ':baseline' = no SSN/DOB cleansing)")
    doc = _template_doc(cleansing=(flag != "baseline"))
    doc["blocking"] = [CANDIDATES[i] for i in VARIANTS[name]]
    if not near:  # blocking measurements never use the near-equal SSN tokens (scoring only), so they need not be computed
        doc["fields"]["ssn"].get("options", {}).pop("near", None)
    cfg = parse_domain_document(doc, source=TEMPLATE)
    register_domain(cfg, replace=True)
    return cfg


# --------------------------------------------------------------------------- the generator
_ONSET = ["b", "d", "f", "g", "h", "k", "l", "m", "n", "p", "r", "s", "t", "v", "w", "z", "br", "st", "tr", "ch", "sh", "kl"]
_VOWEL = ["a", "e", "i", "o", "u", "ai", "ou", "ia"]
_CODA = ["", "n", "r", "s", "l", "m", "k", "t", "nd", "rs"]
_STATES = ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI", "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI"]
_SUFFIXES = ["St", "Ave", "Rd", "Dr", "Ln", "Blvd", "Ct"]
_STREET_BASES = [s.split()[0] for s in STREETS]
SSN_PLACEHOLDERS = ["000-00-0000", "123-45-6789", "111-11-1111", "999-99-9999", "078-05-1120", "000000000"]
DOB_PLACEHOLDERS = ["1900-01-01", "1970-01-01", "0001-01-01", "9999-12-31", "2000-01-01"]
DEFAULT_RATES = {"ssn_rate": 0.7, "dob_rate": 0.9, "phone_rate": 0.8, "email_rate": 0.6}


def synth_name(rank: int, salt: int) -> str:
    """A pronounceable name for a rank in the long tail, deterministic, mostly unique."""
    h = (rank * 2654435761 + salt * 40503) % (2 ** 32)
    parts = []
    for _ in range(2 if rank < 5000 else 3):
        parts.append(_ONSET[h % len(_ONSET)] + _VOWEL[(h // len(_ONSET)) % len(_VOWEL)] + _CODA[(h // 176) % len(_CODA)])
        h = (h * 1103515245 + 12345) % (2 ** 32)
    return "".join(parts).capitalize()


def zm_rank(u: float, rmax: int, q: float) -> int:
    """Rank in 1..rmax with probability proportional to 1/(rank+q) (Zipf-Mandelbrot): a flat-ish head, a long tail."""
    c = (rmax + q) / (1 + q)
    return max(1, min(rmax, int((1 + q) * c ** u - q)))


def _first(rng) -> str:
    if rng.random() < 0.02:
        return rng.choice(FIRST_RARE)
    r = zm_rank(rng.random(), 8000, 5)
    return FIRST_COMMON[r - 1] if r <= len(FIRST_COMMON) else synth_name(r, 1)


def _last(rng) -> str:
    if rng.random() < 0.02:
        return rng.choice(LAST_RARE)
    r = zm_rank(rng.random(), 150000, 20)
    return LAST_COMMON[r - 1] if r <= len(LAST_COMMON) else synth_name(r, 2)


def _place(rng) -> dict:
    r = zm_rank(rng.random(), 33000, 300)
    street_rank = zm_rank(rng.random(), 20000, 40)
    base = _STREET_BASES[street_rank - 1] if street_rank <= len(_STREET_BASES) else synth_name(street_rank, 5)
    return {"address_line1": f"{rng.randint(1, 9999)} {base} {rng.choice(_SUFFIXES)}",
            "city": synth_name(r % 9000, 3) + "ton", "state": _STATES[r % len(_STATES)],
            "postal_code": f"{10000 + (r * 2654435761) % 89999:05d}"}


def _phone(rng) -> str:
    return f"{rng.randint(201, 989)}-{rng.randint(201, 989)}-{rng.randint(1000, 9999)}"


def _ssn(rng) -> str:
    area = rng.choice([a for a in (rng.randint(1, 899), rng.randint(1, 899)) if a != 666])
    return f"{area:03d}-{rng.randint(1, 99):02d}-{rng.randint(1, 9999):04d}"


def _dob(rng) -> str:
    return (date(1935, 1, 1) + timedelta(days=rng.randint(0, 80 * 365))).isoformat()


def make_person(rng, ssn_rate=0.7, dob_rate=0.9, phone_rate=0.8, email_rate=0.6) -> dict:
    first, last = _first(rng), _last(rng)
    p = {"first_name": first, "last_name": last, **_place(rng), "dob": _dob(rng) if rng.random() < dob_rate else "",
         "ssn": _ssn(rng) if rng.random() < ssn_rate else "", "phone": _phone(rng) if rng.random() < phone_rate else "",
         "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@{rng.choice(MAIL_DOMAINS)}" if rng.random() < email_rate else ""}
    return p


def _full(rng) -> dict:
    return make_person(rng, 1, 1, 1, 1)


def _full_date(dob: str):
    """The date, if dob is a complete valid ISO date (not year-only, not empty, not garbled), else None."""
    try:
        return date.fromisoformat(dob) if len(dob) == 10 else None
    except ValueError:
        return None


def _swap_day_month(dob: str) -> str:
    """Day and month exchanged, if that gives a different valid date; otherwise unchanged. Defects stack, so dob may already be damaged."""
    d = _full_date(dob)
    if d is None or d.day > 12 or d.day == d.month:
        return dob
    return date(d.year, d.day, d.month).isoformat()


def _shift_year(dob: str, delta: int) -> str:
    d = _full_date(dob)
    if d is None or not 1901 <= d.year + delta <= 2100:
        return dob
    try:
        return d.replace(year=d.year + delta).isoformat()
    except ValueError:  # Feb 29
        return d.replace(year=d.year + delta, day=28).isoformat()


def _digit_typo(rng, ssn: str) -> str:
    digits = [c for c in ssn if c.isdigit()]
    i = rng.randrange(len(digits))
    digits[i] = rng.choice([c for c in "0123456789" if c != digits[i]])
    s = "".join(digits)
    return f"{s[:3]}-{s[3:5]}-{s[5:]}"


def corrupt(rng, base: dict) -> dict:
    """A second record of the SAME person: 1-3 realistic defects. It always differs from the base in substance: a defect aimed at
    an empty field (dropping an SSN that is not there) changes nothing, so another draw is made."""
    norm = lambda rec: {k: v.lower() for k, v in rec.items()}
    for _ in range(12):
        r = _apply_defects(rng, base)
        if norm(r) != norm(base):
            return r
    return r


def _apply_defects(rng, base: dict) -> dict:
    r = dict(base)

    def nickname(): r["first_name"] = NICKNAMES.get(r["first_name"], typo(rng, r["first_name"], True))
    def last_typo(): r["last_name"] = typo(rng, r["last_name"], rng.random() < 0.5)
    def moved(): r.update(_place(rng))
    def phone_new(): r["phone"] = _phone(rng)
    def phone_swap(): r["phone"] = transpose_digits(rng, r["phone"]) if r["phone"] else _phone(rng)
    def email_new(): r["email"] = f"{r['first_name'].lower()}{rng.randint(1000, 9999)}@{rng.choice(MAIL_DOMAINS)}"
    def ssn_drop(): r["ssn"] = ""
    def ssn_typo(): r["ssn"] = _digit_typo(rng, r["ssn"]) if r["ssn"] else r["ssn"]
    def ssn_placeholder(): r["ssn"] = rng.choice(SSN_PLACEHOLDERS)
    def dob_drop(): r["dob"] = ""
    def dob_swap(): r["dob"] = _swap_day_month(r["dob"])
    def dob_year_typo(): r["dob"] = _shift_year(r["dob"], rng.choice([-2, -1, 1, 2]))
    def dob_year_only(): r["dob"] = r["dob"][:4] if _full_date(r["dob"]) else r["dob"]
    def dob_placeholder(): r["dob"] = rng.choice(DOB_PLACEHOLDERS)

    defects = [nickname, last_typo, moved, phone_new, phone_swap, email_new, ssn_drop, ssn_typo, ssn_placeholder,
               dob_drop, dob_swap, dob_year_typo, dob_year_only, dob_placeholder]
    for f in rng.sample(defects, rng.choice([1, 1, 2, 3])):
        f()
    if rng.random() < 0.3:  # formatting noise standardization should erase
        r["last_name"] = r["last_name"].upper()
    return r


def _seeded(seed: int, i: int):
    return random.Random(seed * 1_000_003 + i)


def is_duplicate(seed: int, i: int, dup_rate: float) -> bool:
    return i > 0 and _seeded(seed, i).random() < dup_rate


def record_at(seed: int, i: int, dup_rate: float = 0.08, **rates):
    """(raw record, truth id, base index or None). Pure in (seed, i). A duplicate is a corrupted copy of an EARLIER record that is
    itself not a duplicate, so truth is just the base index and there are no chains."""
    rates = {**DEFAULT_RATES, **rates}
    rng = _seeded(seed, i)
    if i > 0 and rng.random() < dup_rate:
        for _ in range(8):
            j = rng.randrange(0, i)
            if not is_duplicate(seed, j, dup_rate):
                return corrupt(rng, record_at(seed, j, dup_rate, **rates)[0]), j, j
    return make_person(rng, **rates), i, None


# --------------------------------------------------------------------------- scenarios (labelled cases for the real pipeline)
POSITIVE = ["ssn_exact_name_variants", "ssn_typo", "no_ssn_name_dob_address", "dob_swap", "dob_year_only", "moved_no_ssn", "nickname_dob"]
NEGATIVE = ["ssn_placeholder", "ssn_placeholder_same_name", "ssn_shared_error", "dob_placeholder_same_name", "twins", "jr_sr_dob", "same_name_diff_dob"]


def _dob_before(rng, latest: date) -> str:
    return (date(1935, 1, 1) + timedelta(days=rng.randint(0, (latest - date(1935, 1, 1)).days))).isoformat()


def _dob_swappable(rng) -> str:
    while True:
        d = _dob(rng)
        y, m, dd = d.split("-")
        if int(dd) <= 12 and dd != m:
            return d


def _case(scenario: str, rng):
    """[(raw record, person index)]: records with the same index are the same person."""
    if scenario == "ssn_exact_name_variants":  # only SSN + DOB (+ last name) survive: new nickname, address, phone, email
        p = _full(rng)
        p["first_name"] = rng.choice(list(NICKNAMES))
        q = dict(p, first_name=NICKNAMES[p["first_name"]], **_place(rng), phone=_phone(rng), email=f"x{rng.randint(1000, 9999)}@{rng.choice(MAIL_DOMAINS)}")
        return [(p, 0), (q, 0)]
    if scenario == "ssn_typo":  # one wrong digit; everything else identical
        p = _full(rng)
        return [(p, 0), (dict(p, ssn=_digit_typo(rng, p["ssn"])), 0)]
    if scenario == "no_ssn_name_dob_address":
        p = dict(_full(rng), ssn="", phone="", email="")
        return [(p, 0), (dict(p, last_name=typo(rng, p["last_name"], False)), 0)]
    if scenario == "dob_swap":
        p = dict(_full(rng), ssn="", phone="", email="", dob=_dob_swappable(rng))
        return [(p, 0), (dict(p, dob=_swap_day_month(p["dob"])), 0)]
    if scenario == "dob_year_only":
        p = dict(_full(rng), ssn="", phone="", email="")
        return [(p, 0), (dict(p, dob=p["dob"][:4]), 0)]
    if scenario == "moved_no_ssn":  # same name + DOB, new address and phone, nothing else
        p = dict(_full(rng), ssn="", email="")
        return [(p, 0), (dict(p, **_place(rng), phone=_phone(rng)), 0)]
    if scenario == "nickname_dob":
        p = dict(_full(rng), ssn="", phone="", email="", first_name=rng.choice(list(NICKNAMES)))
        return [(p, 0), (dict(p, first_name=NICKNAMES[p["first_name"]]), 0)]
    if scenario == "ssn_placeholder":  # strangers sharing a junk SSN
        junk = rng.choice(SSN_PLACEHOLDERS)
        return [(dict(_full(rng), ssn=junk), 0), (dict(_full(rng), ssn=junk), 1)]
    if scenario == "ssn_placeholder_same_name":
        junk, a = rng.choice(SSN_PLACEHOLDERS), dict(_full(rng), first_name=rng.choice(FIRST_COMMON[:8]), last_name=rng.choice(LAST_COMMON[:8]))
        b = dict(_full(rng), first_name=a["first_name"], last_name=a["last_name"])
        return [(dict(a, ssn=junk), 0), (dict(b, ssn=junk), 1)]
    if scenario == "ssn_shared_error":  # a real SSN typed onto the wrong person
        a, b = _full(rng), _full(rng)
        return [(a, 0), (dict(b, ssn=a["ssn"]), 1)]
    if scenario == "dob_placeholder_same_name":  # two John Smiths in one town, both with a defaulted DOB
        junk = rng.choice(DOB_PLACEHOLDERS)
        a = dict(_full(rng), first_name=rng.choice(FIRST_COMMON[:8]), last_name=rng.choice(LAST_COMMON[:8]), ssn="", phone="", email="", dob=junk)
        b = dict(a, **_place(rng))
        b["postal_code"], b["city"], b["state"] = a["postal_code"], a["city"], a["state"]
        return [(a, 0), (b, 1)]
    if scenario == "twins":  # one household, same DOB, different first names
        a = dict(_full(rng), ssn="")
        return [(a, 0), (dict(a, first_name=rng.choice([n for n in FIRST_COMMON if n[0] != a["first_name"][0]]), ssn=""), 1)]
    if scenario == "jr_sr_dob":
        a = dict(_full(rng), ssn="")
        a["dob"] = _dob_before(rng, date(1998, 12, 31))  # the son is 28 years younger, and must not be born in the future
        a["last_name"] = f"{a['last_name']} Sr"
        b = dict(a, last_name=a["last_name"][:-2] + "Jr", dob=(date.fromisoformat(a["dob"]) + timedelta(days=365 * 28)).isoformat(), phone=_phone(rng))
        return [(a, 0), (b, 1)]
    if scenario == "same_name_diff_dob":
        a = dict(_full(rng), first_name=rng.choice(FIRST_COMMON[:8]), last_name=rng.choice(LAST_COMMON[:8]), ssn="", phone="", email="")
        b = dict(a, dob=_dob(rng), address_line1=_place(rng)["address_line1"])
        return [(a, 0), (b, 1)]
    raise ValueError(scenario)


def generate(seed: int, scale: int = 25):
    """scale = cases per scenario; background = 10 x scale single-record people (same shape as app/benchmark.generate)."""
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
        rows.append({"raw": make_person(rng, **DEFAULT_RATES), "true": true_id, "scenario": "background", "case": f"background:{i}"})
        true_id += 1
    rng.shuffle(rows)
    return rows


# --------------------------------------------------------------------------- the scale harness
_STREAM = {}  # set before forking so workers see it


class MeasurementPaused(Exception):
    """A measurement stopped at its time budget and saved its progress; running the same command again continues it."""

    def __init__(self, done: int, total: int):
        super().__init__(f"paused after {done:,} of {total:,} records")
        self.done, self.total = done, total


def _group_of(key: str) -> int:
    return int(key[1:].split(":", 1)[0])


def keys_of_record(raw: dict, cfg) -> list:
    return [k for k in blocking_keys(standardize(DOMAIN, raw), cfg) if k != "__no_block__"]


def _stream_range(span):
    """Counts, for the watched keys only, how many records in [lo, hi) carry them (members kept up to cap+1)."""
    lo, hi = span
    watched, cap, cfg, seed, dup_rate, rates = (_STREAM[k] for k in ("watched", "cap", "cfg", "seed", "dup_rate", "rates"))
    counts, members = {}, {}
    for i in range(lo, hi):
        for k in keys_of_record(record_at(seed, i, dup_rate, **rates)[0], cfg):
            if k in watched:
                counts[k] = counts.get(k, 0) + 1
                m = members.setdefault(k, [])
                if len(m) <= cap:
                    m.append(i)
    return counts, members


def _stream_all(n, watched, cap, cfg, seed, dup_rate, rates, workers, checkpoint=None, deadline=None):
    _STREAM.update(watched=watched, cap=cap, cfg=cfg, seed=seed, dup_rate=dup_rate, rates=rates)
    chunk = max(1000, min(25000, n // max(workers, 1) // 4 or 1000))
    spans = [(lo, min(lo + chunk, n)) for lo in range(0, n, chunk)]
    ident = (n, seed, dup_rate, cap, tuple(sorted(rates.items())), len(watched), chunk)
    counts, members, first = {}, {}, 0
    if checkpoint and os.path.exists(checkpoint):
        with open(checkpoint, "rb") as f:
            saved = pickle.load(f)
        if saved["id"] == ident:  # progress from an earlier, interrupted run of exactly this measurement
            counts, members, first = saved["counts"], saved["members"], saved["next"]

    def merge(c, m):
        for k, v in c.items():
            counts[k] = counts.get(k, 0) + v
        for k, v in m.items():
            lst = members.setdefault(k, [])
            if len(lst) <= cap:
                lst.extend(v[:cap + 1 - len(lst)])

    if workers > 1 and not checkpoint:
        with multiprocessing.get_context("fork").Pool(workers) as pool:
            for c, m in pool.map(_stream_range, spans):
                merge(c, m)
        return counts, members
    for idx in range(first, len(spans)):
        merge(*_stream_range(spans[idx]))
        if deadline is not None and time.time() > deadline and idx + 1 < len(spans):
            with open(checkpoint, "wb") as f:
                pickle.dump({"id": ident, "counts": counts, "members": members, "next": idx + 1}, f)
            raise MeasurementPaused(spans[idx][1], n)
    if checkpoint and os.path.exists(checkpoint):
        os.remove(checkpoint)  # finished
    return counts, members


def _pct(sorted_values, p):
    return sorted_values[min(len(sorted_values) - 1, int(len(sorted_values) * p))] if sorted_values else 0


def measure_size(n: int, *, seed=7, sample=2000, dup_sample=1000, dup_rate=0.08, cap=500, variants=None, rates=None, workers=1,
                 checkpoint=None, budget=None) -> dict:
    """Partners per record at n records, for each blocking variant, from a sample of records. Exact for the sample: a record's
    partners are the DISTINCT other records sharing at least one key whose bucket is not over the cap."""
    rates = {**DEFAULT_RATES, **(rates or {})}
    variants = variants or list(VARIANTS)
    cfg = register_person("full", near=False)  # key labels carry the index of their group in CANDIDATES, so one pass serves every variant
    t0 = time.time()
    deadline = t0 + budget if budget is not None else None
    rng = random.Random(seed ^ n)
    sample_idx = rng.sample(range(n), min(sample, n))
    # blocking recall needs duplicates, which are only 8% of records: sample them on purpose instead of hoping the main sample has enough
    dups = [i for i in range(n) if is_duplicate(seed, i, dup_rate)]
    dup_idx = rng.sample(dups, min(dup_sample, len(dups)))
    dup_pairs = [(i, record_at(seed, i, dup_rate, **rates)[2]) for i in dup_idx]
    need = set(sample_idx) | set(dup_idx) | {j for _, j in dup_pairs}
    keys = {x: keys_of_record(record_at(seed, x, dup_rate, **rates)[0], cfg) for x in need}
    counts, members = _stream_all(n, {k for ks in keys.values() for k in ks}, cap, cfg, seed, dup_rate, rates, workers, checkpoint, deadline)

    def usable(k):  # the engine skips a key when the OTHER records carrying it outnumber the cap (pipeline._route_record)
        return counts.get(k, 0) - 1 <= cap

    out = {"records": n, "seconds": round(time.time() - t0, 1), "sample": len(sample_idx), "duplicates_in_sample": len(dup_pairs),
           "records_per_second": round(n / max(time.time() - t0, 1e-9)), "variants": {}, "by_group": {}}
    for name in variants:
        groups = set(VARIANTS[name])
        partners, unreachable, keys_seen, keys_over = [], 0, 0, 0
        for i in sample_idx:
            seen, live = set(), 0
            for k in keys[i]:
                if _group_of(k) not in groups:
                    continue
                keys_seen += 1
                if not usable(k):
                    keys_over += 1
                    continue
                live += 1
                seen.update(members[k])
            seen.discard(i)
            partners.append(len(seen))
            unreachable += live == 0
        found = sum(1 for i, j in dup_pairs if any(_group_of(k) in groups and usable(k) and k in set(keys[j]) for k in keys[i]))
        partners.sort()
        mean = sum(partners) / len(partners)
        out["variants"][name] = {"partners_mean": round(mean, 1), "partners_median": partners[len(partners) // 2], "partners_p95": _pct(partners, 0.95),
                                 "partners_max": partners[-1], "pairs_per_record": round(mean / 2, 1), "unreachable_pct": round(100 * unreachable / len(partners), 2),
                                 "keys_over_cap_pct": round(100 * keys_over / max(keys_seen, 1), 2),
                                 "blocking_recall": round(found / len(dup_pairs), 4) if dup_pairs else None}
    for g, label in enumerate(GROUP_LABELS):  # each group on its own, so the culprit is visible
        ps = []
        for i in sample_idx:
            seen = set()
            for k in keys[i]:
                if _group_of(k) == g and usable(k):
                    seen.update(members[k])
            seen.discard(i)
            ps.append(len(seen))
        out["by_group"][label] = round(sum(ps) / len(ps), 1)
    return out


def add_growth(report: dict) -> dict:
    """Growth exponent between consecutive sizes: partners ~ records^a  (a=1 linear: bad; a=0 flat: good)."""
    import math
    sizes = report["sizes"] = sorted(report["sizes"], key=lambda x: x["records"])
    for name in sizes[0]["variants"]:
        for a, b in zip(sizes, sizes[1:]):
            pa, pb = a["variants"][name]["partners_mean"], b["variants"][name]["partners_mean"]
            b["variants"][name].pop("growth_exponent", None)
            if pa > 0 and pb > 0:
                b["variants"][name]["growth_exponent"] = round(math.log(pb / pa) / math.log(b["records"] / a["records"]), 2)
    return report


def scale_profile(sizes, *, log=print, base: dict | None = None, **kw) -> dict:
    """Measures each size. With `base` (an earlier report) only the sizes it lacks are measured: a 10M run can be added to a
    10k/100k/1M report without redoing them. The settings must match, or the sizes are not comparable, so they are checked."""
    settings = {k: v for k, v in kw.items() if k not in ("workers", "checkpoint", "budget")} | {"rates": {**DEFAULT_RATES, **(kw.get("rates") or {})}}
    report = base or {"settings": settings, "sizes": []}
    if base:
        for key in ("seed", "cap", "dup_rate", "rates"):
            if base["settings"].get(key) != settings.get(key) and settings.get(key) is not None:
                raise ValueError(f"cannot extend: '{key}' differs ({base['settings'].get(key)} vs {settings.get(key)}), the sizes would not be comparable")
    have = {s["records"] for s in report["sizes"]}
    for n in sizes:
        if n in have:
            log(f"  {n:,} records already measured")
            continue
        log(f"  measuring {n:,} records ...")
        report["sizes"].append(measure_size(n, **kw))
        log(f"    done in {report['sizes'][-1]['seconds']}s")
    return add_growth(report)


def format_scale(report: dict) -> str:
    s = report["settings"]
    lines = [f"Partners per record by blocking variant  (cap {s.get('cap', 500)} per key; {s.get('sample', 2000)} records sampled per size; "
             f"recall on {report['sizes'][0]['duplicates_in_sample']} sampled duplicates; SSN present {s['rates']['ssn_rate']:.0%}, DOB {s['rates']['dob_rate']:.0%}, phone {s['rates']['phone_rate']:.0%}, email {s['rates']['email_rate']:.0%}; "
             f"{s.get('dup_rate', 0.08):.0%} of records are duplicates)", "",
             "  partners = DISTINCT other records sharing a usable key; pairs to score per record is half of that.",
             "  growth exponent: partners ~ records^a.  a=1 means total work grows as the SQUARE of the data; a=0 means it stays flat.", ""]
    for name in report["sizes"][0]["variants"]:
        lines.append(f"{name}  (groups: {', '.join(GROUP_LABELS[i] for i in VARIANTS[name])})")
        lines.append(f"  {'records':>10} {'mean':>8} {'median':>7} {'p95':>6} {'max':>6} {'pairs/rec':>10} {'growth a':>9} {'no usable key':>14} {'keys over cap':>14} {'recall':>7}")
        for sz in report["sizes"]:
            v = sz["variants"][name]
            lines.append(f"  {sz['records']:>10,} {v['partners_mean']:>8} {v['partners_median']:>7} {v['partners_p95']:>6} {v['partners_max']:>6} {v['pairs_per_record']:>10} "
                         f"{v.get('growth_exponent', ''):>9} {v['unreachable_pct']:>13}% {v['keys_over_cap_pct']:>13}% "
                         f"{'' if v['blocking_recall'] is None else format(v['blocking_recall'], '.1%'):>7}")
        lines.append("")
    last = report["sizes"][-1]
    lines.append(f"Each group ALONE at {last['records']:,} records (mean partners):")
    for label, v in sorted(last["by_group"].items(), key=lambda x: -x[1]):
        lines.append(f"  {v:>9}  {label}")
    return "\n".join(lines)


# --------------------------------------------------------------------------- the bulk engine on a generated population, scored against the truth
def bulk_trial(size: int, workdir: str, *, seed=7, dup_rate=0.08, rates=None, workers=1, chunk=2000, budget=None, write=False, variant="scalable_tight_plus", log=print):
    """Runs the bulk engine (app/bulk.py) on `size` generated people (the SAME population measure_size samples) and, when it finishes, scores
    what it did against the truth the generator knows. Resumable: with `budget` it raises bulk.BulkPaused at a chunk boundary and the same call
    continues where it stopped. write=False is a dry run (stages A to D)."""
    from .bulk import bulk_load
    rates = {**DEFAULT_RATES, **(rates or {})}
    register_person(variant)
    os.makedirs(workdir, exist_ok=True)
    params = {"size": size, "seed": seed, "dup_rate": dup_rate, "rates": rates, "variant": variant}
    marker = os.path.join(workdir, "trial.json")
    if os.path.exists(marker):  # a work directory holds ONE trial: resuming it with other parameters would score the wrong data
        with open(marker) as f:
            if json.load(f) != params:
                raise ValueError(f"{workdir} holds a trial with other parameters; use an empty directory (or the same parameters to resume it)")
    else:
        with open(marker, "w") as f:
            json.dump(params, f)
    store = Store(os.path.join(workdir, "trial_store.db"))
    source = lambda start: (("synthetic", f"r{i}", record_at(seed, i, dup_rate, **rates)[0]) for i in range(start, size))
    result = bulk_load(store, "trial", DOMAIN, source, workdir=os.path.join(workdir, "work"), resume=True, budget=budget, write=write, chunk=chunk, workers=workers, log=log)
    return score_bulk_result(result, size, seed, dup_rate, rates)


def score_bulk_result(result, size: int, seed: int, dup_rate: float, rates: dict) -> dict:
    """Exact numbers for EVERY record (the scale harness only samples): pairs, and how the true duplicates fared at each step."""
    work = result.workdir
    work_db, pairs_db, edges_db = (sqlite3.connect(os.path.join(work, n)) for n in ("work.db", "pairs.db", "edges.db"))
    dups = [i for i in range(size) if is_duplicate(seed, i, dup_rate)]
    base = {i: record_at(seed, i, dup_rate, **rates)[2] for i in dups}
    truth = lambda i: base.get(i, i)  # the generator's truth is just this: a duplicate is the person its base record is, every other record is itself
    decision_of = {(a, b): d for a, b, d in edges_db.execute("SELECT a, b, decision FROM edges")}  # (an unindexed lookup per duplicate scanned every edge)
    found = {"candidate": 0, "auto": 0, "review": 0, "same_entity": 0, "never_compared": 0, "compared_but_rejected": 0}
    for i in dups:
        a, b = min(i, base[i]), max(i, base[i])
        in_pairs = pairs_db.execute("SELECT 1 FROM pairs WHERE a = ? AND b = ?", (a, b)).fetchone() is not None
        edge = decision_of.get((a, b))
        roots = dict(work_db.execute("SELECT idx, root FROM comp WHERE idx IN (?, ?)", (a, b)).fetchall())
        found["candidate"] += in_pairs
        found["never_compared"] += not in_pairs
        found["auto"] += edge == "auto_merge"
        found["review"] += edge == "review"
        found["compared_but_rejected"] += bool(in_pairs and not edge)
        found["same_entity"] += roots.get(a) == roots.get(b)
    impure = false_records = 0
    for root, members in work_db.execute("SELECT root, GROUP_CONCAT(idx) FROM comp GROUP BY root HAVING COUNT(*) > 1"):
        ids = [int(x) for x in members.split(",")]
        counts = {}
        for i in ids:
            counts[truth(i)] = counts.get(truth(i), 0) + 1
        if len(counts) > 1:
            impure += 1
            false_records += len(ids) - max(counts.values())
    review_total = review_true = 0
    for (a, b), decision in decision_of.items():
        if decision == "review":
            review_total += 1
            review_true += truth(a) == truth(b)
    for con in (work_db, pairs_db, edges_db):
        con.close()
    n = len(dups)
    return {"records": result.records, "entities": result.entities, "true_entities": size - n, "true_duplicate_pairs": n,
            "candidate_pairs": result.candidate_pairs, "pairs_per_record": round(result.candidate_pairs / max(result.records, 1), 3),
            "auto_pairs": result.auto_pairs, "review_pairs": result.review_pairs, "capped_keys": result.capped_keys, "seconds": result.seconds,
            "duplicates": {k: v for k, v in found.items()}, "blocking_recall": round(found["candidate"] / max(n, 1), 5),
            "merge_recall": round(found["same_entity"] / max(n, 1), 5), "impure_clusters": impure, "falsely_merged_records": false_records,
            "review_precision": round(review_true / max(review_total, 1), 4), "dry_run": result.dry_run}


def format_bulk_trial(r: dict, before: dict | None = None) -> str:
    sec = r["seconds"]
    work = sec["standardize"] + sec["pairs"] + sec["score"] + sec["cluster"]
    lines = [f"Bulk engine on {r['records']:,} generated people ({'dry run: stages A-D' if r['dry_run'] else 'full load'}), scored against the generator's truth", "",
             f"  stage seconds     standardize {sec['standardize']}   pairs {sec['pairs']}   score {sec['score']}   cluster {sec['cluster']}" + (f"   write {sec['write']}" if "write" in sec else ""),
             f"  stages A-D        {work:,.0f} s  =  {r['records'] / max(work, 1e-9):,.0f} records/s on one core",
             "", f"  candidate pairs   {r['candidate_pairs']:,}   ({r['pairs_per_record']} per record; {r['auto_pairs']:,} auto-merge, {r['review_pairs']:,} review; {r['capped_keys']} keys over the cap)",
             f"  entities          {r['entities']:,}   (the truth has {r['true_entities']:,}: {r['entities'] - r['true_entities']:+,})",
             f"  true duplicates   {r['true_duplicate_pairs']:,} pairs: {r['duplicates']['candidate']:,} compared, {r['duplicates']['auto']:,} auto-merged, {r['duplicates']['review']:,} sent to review,"
             f" {r['duplicates']['compared_but_rejected']:,} compared but scored too low, {r['duplicates']['never_compared']:,} never compared",
             f"  blocking recall   {r['blocking_recall']:.3%}  (share of true duplicate pairs that were compared)",
             f"  merge recall      {r['merge_recall']:.3%}  (share that ended up in one entity)",
             f"  false merges      {r['impure_clusters']:,} entities hold records of different people ({r['falsely_merged_records']:,} records)",
             f"  review precision  {r['review_precision']:.1%}  (share of review pairs that really are the same person)"]
    if before:
        lines += ["", f"  BEFORE (the scale harness: a SAMPLE of {before['sample']:,} records and {before['dups']:,} duplicates, same population and key set)",
                  f"    pairs per record   {before['pairs_per_record']}   now exact: {r['pairs_per_record']}",
                  f"    blocking recall    {before['blocking_recall']:.2%}   now exact: {r['blocking_recall']:.2%}"]
    return "\n".join(lines)


# --------------------------------------------------------------------------- the two engines on the same population
class EnginesPaused(Exception):
    def __init__(self, done: int, total: int):
        super().__init__(f"paused after {done:,} of {total:,} records in the regular engine")
        self.done, self.total = done, total


def store_state(path: str):
    """What an engine left in a database, in a form independent of the ids it generated: (entities as sets of record numbers,
    review pairs, auto-merged pairs, golden records keyed by their source records)."""
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    idx_of, source_of, groups, sources_of = {}, {}, {}, {}
    for r in con.execute("SELECT id, source_record_id, entity_id FROM source_records"):
        idx_of[r["id"]], source_of[r["id"]] = int(r["source_record_id"][1:]), r["source_record_id"]
        groups.setdefault(r["entity_id"], set()).add(int(r["source_record_id"][1:]))
        sources_of.setdefault(r["entity_id"], set()).add(r["source_record_id"])
    pairs = {"pending": set(), "confirmed": set()}
    for r in con.execute("SELECT record_a_id, record_b_id, status FROM match_candidates"):
        if r["status"] in pairs:
            pairs[r["status"]].add(frozenset((idx_of[r["record_a_id"]], idx_of[r["record_b_id"]])))
    golden = {}
    for e in con.execute(f"SELECT * FROM entity_{DOMAIN} WHERE status = 'active'"):
        row = {k: e[k] for k in e.keys() if k not in ("entity_id", "created_at", "updated_at")}
        if row.get("quality_issues"):  # an issue names its record by id; the ids differ between engines, the source ids do not
            row["quality_issues"] = [{**i, "record_id": source_of.get(i["record_id"], i["record_id"])} for i in json.loads(row["quality_issues"])]
        golden[frozenset(sources_of.get(e["entity_id"], ()))] = row   # (one pass over the records: a query per entity was quadratic)
    con.close()
    return {frozenset(g) for g in groups.values()}, pairs["pending"], pairs["confirmed"], golden


def score_store(state, size: int, seed: int, dup_rate: float, rates: dict) -> dict:
    """How an engine's result compares with the truth the generator knows."""
    entities, review, auto, _ = state
    dups = [i for i in range(size) if is_duplicate(seed, i, dup_rate)]
    base = {i: record_at(seed, i, dup_rate, **rates)[2] for i in dups}
    entity_of = {i: g for g in entities for i in g}
    truth = lambda i: base.get(i, i)
    together = sum(entity_of[i] is entity_of[base[i]] for i in dups)
    impure = [g for g in entities if len({truth(i) for i in g}) > 1]
    return {"entities": len(entities), "true_entities": size - len(dups), "auto_pairs": len(auto), "review_pairs": len(review),
            "true_duplicate_pairs": len(dups), "merge_recall": round(together / max(len(dups), 1), 5), "impure_entities": len(impure),
            "falsely_merged_records": sum(len(g) - max(sum(1 for i in g if truth(i) == t) for t in {truth(i) for i in g}) for g in impure),
            "review_precision": round(sum(1 for p in review if len({truth(i) for i in p}) == 1) / max(len(review), 1), 4)}


def engines_trial(size: int, workdir: str, *, seed=7, dup_rate=0.08, rates=None, budget=None, chunk=200, variant="scalable_tight_plus", log=print) -> dict:
    """The SAME generated population through the regular engine (run_batch, the path bulk_ingest.py takes without --bulk) and the bulk engine
    (written to the store, so its write stage is timed too); then both results are scored against the truth and compared with each other.
    The regular engine is resumable (`budget` seconds per call, EnginesPaused); the bulk one is quick and runs once."""
    from .bulk import bulk_load
    from .pipeline import run_batch
    rates = {**DEFAULT_RATES, **(rates or {})}
    register_person(variant)
    os.makedirs(workdir, exist_ok=True)
    params = {"size": size, "seed": seed, "dup_rate": dup_rate, "rates": rates, "variant": variant}
    marker, times_path = os.path.join(workdir, "engines.json"), os.path.join(workdir, "times.json")
    if os.path.exists(marker):
        with open(marker) as f:
            if json.load(f) != params:
                raise ValueError(f"{workdir} holds a comparison with other parameters; use an empty directory")
    else:
        with open(marker, "w") as f:
            json.dump(params, f)
    times = {"regular": 0.0, "regular_done": False, "bulk": None}
    if os.path.exists(times_path):
        with open(times_path) as f:
            times = json.load(f)

    def save():
        with open(times_path, "w") as f:
            json.dump(times, f)
    people = lambda lo, hi: [("synthetic", f"r{i}", record_at(seed, i, dup_rate, **rates)[0]) for i in range(lo, hi)]
    bulk_path, reg_path = os.path.join(workdir, "bulk.db"), os.path.join(workdir, "regular.db")
    if times["bulk"] is None:
        for path in (bulk_path,):
            if os.path.exists(path):
                os.remove(path)
        store, t0 = Store(bulk_path), time.time()
        result = bulk_load(store, "trial", DOMAIN, lambda start: iter(people(start, size)), chunk=2000, log=log)
        times["bulk"] = {"total": round(time.time() - t0, 2), "stages": result.seconds}
        save()
    if not times["regular_done"]:
        store = Store(reg_path)
        done = sqlite3.connect(reg_path).execute("SELECT COUNT(*) FROM source_records").fetchone()[0]
        t0 = time.time()
        for lo in range(max(0, done - chunk), size, chunk):  # re-sending the chunk that was in flight finishes whatever it left unfinished
            run_batch(store, "trial", DOMAIN, people(lo, min(lo + chunk, size)))
            # pause only once this call has handled records the store did not hold before: re-sending the chunk that was in flight is not progress
            if budget is not None and time.time() - t0 > budget and lo + chunk > done and lo + chunk < size:
                times["regular"] = round(times["regular"] + time.time() - t0, 2)
                save()
                raise EnginesPaused(lo + chunk, size)
        times["regular"] = round(times["regular"] + time.time() - t0, 2)
        times["regular_done"] = True
        save()
    states = {"regular": store_state(reg_path), "bulk": store_state(bulk_path)}
    out = {"records": size, "regular": {**score_store(states["regular"], size, seed, dup_rate, rates), "seconds": times["regular"]},
           "bulk": {**score_store(states["bulk"], size, seed, dup_rate, rates), "seconds": times["bulk"]["total"], "stages": times["bulk"]["stages"]}}
    out["identical"] = {"entities": states["regular"][0] == states["bulk"][0], "review_pairs": states["regular"][1] == states["bulk"][1],
                        "auto_pairs": states["regular"][2] == states["bulk"][2], "golden_records": states["regular"][3] == states["bulk"][3]}
    out["speedup"] = round(times["regular"] / max(times["bulk"]["total"], 1e-9), 1)
    return out


def format_engines_trial(r: dict) -> str:
    a, b, n = r["regular"], r["bulk"], r["records"]
    rate = lambda t: f"{n / max(t, 1e-9):,.0f} records/s"
    st = b["stages"]
    rows = [("time", f"{a['seconds']:,.0f} s  ({rate(a['seconds'])})", f"{b['seconds']:,.0f} s  ({rate(b['seconds'])})"),
            ("entities (truth: %s)" % f"{a['true_entities']:,}", f"{a['entities']:,}", f"{b['entities']:,}"),
            ("auto-merged pairs", f"{a['auto_pairs']:,}", f"{b['auto_pairs']:,}"), ("review pairs", f"{a['review_pairs']:,}", f"{b['review_pairs']:,}"),
            ("true duplicates merged", f"{a['merge_recall']:.2%}", f"{b['merge_recall']:.2%}"),
            ("entities with different people", f"{a['impure_entities']:,}", f"{b['impure_entities']:,}"),
            ("review precision", f"{a['review_precision']:.1%}", f"{b['review_precision']:.1%}")]
    lines = [f"The same {n:,} generated people through both engines, scored against the generator's truth", "",
             f"  {'':32s}{'regular engine':>30s}{'bulk engine':>30s}"] + [f"  {k:32s}{x:>30s}{y:>30s}" for k, x, y in rows]
    lines += ["", f"  bulk stages: standardize {st['standardize']} s, pairs {st['pairs']} s, score {st['score']} s, cluster {st['cluster']} s, WRITE {st.get('write', 0)} s",
              f"  bulk engine is {r['speedup']}x faster, end to end (writes included)", "",
              "  identical results: " + ", ".join(f"{k.replace('_', ' ')} {'yes' if v else 'NO'}" for k, v in r["identical"].items())]
    return "\n".join(lines)


# --------------------------------------------------------------------------- a delta onto existing data, at scale
class DeltaPaused(Exception):
    def __init__(self, done: int, total: int):
        super().__init__(f"paused after {done} of {total} pieces")
        self.done, self.total = done, total


def delta_invariants(path: str) -> dict:
    con = sqlite3.connect(path)
    one = lambda sql: con.execute(sql).fetchone()[0]
    out = {"records": one("SELECT COUNT(*) FROM source_records"), "records_without_an_entity": one("SELECT COUNT(*) FROM source_records WHERE entity_id IS NULL"),
           "records_not_finished": one("SELECT COUNT(*) FROM source_records WHERE processing_status <> 'matched'"),
           "active_entities": one(f"SELECT COUNT(*) FROM entity_{DOMAIN} WHERE status = 'active'"), "merged_entities": one(f"SELECT COUNT(*) FROM entity_{DOMAIN} WHERE status = 'merged'"),
           "history_rows": one("SELECT COUNT(*) FROM source_record_history"), "records_at_version_2": one("SELECT COUNT(*) FROM source_records WHERE version = 2"),
           # set operations, not a subquery per entity: source_records.entity_id has no index, so a probe per entity scans the whole table each time
           "active_entities_without_records": one(f"SELECT COUNT(*) FROM entity_{DOMAIN} WHERE status = 'active' AND entity_id NOT IN (SELECT entity_id FROM source_records WHERE entity_id IS NOT NULL)"),
           "records_in_a_merged_entity": one(f"SELECT COUNT(*) FROM source_records WHERE entity_id IN (SELECT entity_id FROM entity_{DOMAIN} WHERE status <> 'active')"),
           "database_bytes": os.path.getsize(path)}
    con.close()
    return out


def delta_trial(base: int, feed: int, workdir: str, *, seed=7, dup_rate=0.08, rates=None, piece=50_000, delta_piece=None, budget=None, workers=1, variant="scalable_tight_plus", log=print) -> dict:
    """A DELTA onto existing data, at scale. `base` people are loaded (the first piece as a bulk load, the rest onboarded); then a feed of `feed` records
    (80% new people, 20% existing people whose email, phone, last name or address changed) is applied as deltas of `piece` records. Each piece is a complete
    load, so the pieces' stage times add up to the cost of the whole feed, and a piece fits in one call. `delta_piece` (default: `piece`) may be made SMALLER between
    calls: the cost of a piece grows with the data already held, so late pieces can outgrow a call. Resumable: `budget` seconds per call (DeltaPaused),
    and a call that died MID-WRITE is detected rather than built on."""
    from .bulk import bulk_load
    assert feed % 5 == 0, "feed must be a multiple of 5: every fifth record is a change"
    rates = {**DEFAULT_RATES, **(rates or {})}
    register_person(variant)
    os.makedirs(workdir, exist_ok=True)
    params = {"base": base, "feed": feed, "seed": seed, "dup_rate": dup_rate, "rates": rates, "piece": piece, "variant": variant}
    marker, state_path = os.path.join(workdir, "delta.json"), os.path.join(workdir, "pieces.json")
    if os.path.exists(marker):
        with open(marker) as f:
            if json.load(f) != params:
                raise ValueError(f"{workdir} holds a trial with other parameters; use an empty directory")
    else:
        with open(marker, "w") as f:
            json.dump(params, f)
    state = {"pieces": [], "in_progress": None}
    if os.path.exists(state_path):
        with open(state_path) as f:
            state = json.load(f)

    def save():
        with open(state_path, "w") as f:
            json.dump(state, f)
    if state["in_progress"] is not None:  # (piece 0 is a real piece: a bare truth test would miss it)
        raise RuntimeError(f"a previous call died while loading piece {state['in_progress']}: the store may be partly written, so nothing can be built on it. Start again in an empty directory.")
    raw = lambda i: record_at(seed, i, dup_rate, **rates)[0]
    n_changed = feed // 5
    step = max(1, base // n_changed)

    def changed(c):  # the c-th change: an existing person whose details moved on
        i, attrs, kind = c * step, dict(raw(c * step)), c % 4
        if kind == 0:
            attrs["email"] = f"moved{c}@newmail.test"
        elif kind == 1:
            attrs["phone"] = f"{200 + c % 700}-555-{c % 10000:04d}"
        elif kind == 2:
            attrs["last_name"] = (attrs.get("last_name") or "Lee") + "s"
        else:
            attrs["address_line1"] = f"{c % 9000 + 1} Newer Street"
        return ("synthetic", f"r{i}", attrs)

    def feed_item(j):
        return changed(j // 5) if j % 5 == 0 else ("synthetic", f"r{base + j - j // 5 - 1}", raw(base + j - j // 5 - 1))
    base_pieces = [("base", lo, min(base, lo + piece)) for lo in range(0, base, piece)]
    size = delta_piece or piece
    store, started, ran = Store(os.path.join(workdir, "trial.db")), time.time(), 0
    while True:
        number = len(state["pieces"])
        if number < len(base_pieces):
            kind, lo, hi = base_pieces[number]
        else:
            done = sum(p["records"] for p in state["pieces"] if p["kind"] == "delta")
            if done >= feed:
                break
            kind, lo, hi = "delta", done, min(feed, done + size)
        total = len(base_pieces) + sum(1 for p in state["pieces"] if p["kind"] == "delta") + -(-(feed - sum(p["records"] for p in state["pieces"] if p["kind"] == "delta")) // size)
        per_record = max((p["wall_seconds"] / p["records"] for p in [q for q in state["pieces"] if q["kind"] == kind][-3:]), default=None)   # a record costs MORE as the data held grows, and a cold cache can double it: the worst of the last three
        if budget is not None and ran and per_record and (time.time() - started) + per_record * (hi - lo) * 1.4 > budget:
            raise DeltaPaused(number, total)
        state["in_progress"] = number
        save()
        t_wall = time.time()
        records = [("synthetic", f"r{i}", raw(i)) for i in range(lo, hi)] if kind == "base" else [feed_item(j) for j in range(lo, hi)]
        t = time.time()
        result = bulk_load(store, "trial", DOMAIN, records, workers=workers, chunk=2000, onboard=(kind == "base" and lo > 0), update=(kind == "delta"))
        engine = time.time() - t
        state["pieces"].append({"kind": kind, "lo": lo, "hi": hi, "records": len(records), "engine_seconds": round(engine, 1), "wall_seconds": round(time.time() - t_wall, 1), "stages": result.seconds,
                                "new": result.new_records, "changed": result.changed_records, "unchanged": result.unchanged_records, "pairs": result.candidate_pairs,
                                "auto": result.auto_pairs, "review": result.review_pairs, "touched": result.existing_entities_touched, "merged": result.existing_entities_merged,
                                "new_entities": result.entities})
        state["in_progress"] = None
        save()
        ran += 1
        log(f"piece {number + 1}/{total} ({kind} {lo:,}-{hi:,}, {len(records):,} records): {engine:.0f}s")
    pieces = state["pieces"]
    deltas = [p for p in pieces if p["kind"] == "delta"]
    stage = lambda ps, k: round(sum(p["stages"].get(k, 0) for p in ps), 1)
    keys = ("classify", "standardize", "pairs", "score", "cluster", "write")
    return {"base": base, "feed": feed, "piece": piece, "n_pieces": len(pieces),
            "base_load": {"seconds": round(sum(p["engine_seconds"] for p in pieces if p["kind"] == "base"), 1), "records": base},
            "delta": {"records": feed, "seconds": round(sum(p["engine_seconds"] for p in deltas), 1), "stages": {k: stage(deltas, k) for k in keys},
                      "new": sum(p["new"] for p in deltas), "changed": sum(p["changed"] for p in deltas), "unchanged": sum(p["unchanged"] for p in deltas),
                      "pairs": sum(p["pairs"] for p in deltas), "auto": sum(p["auto"] for p in deltas), "review": sum(p["review"] for p in deltas),
                      "existing_entities_touched": sum(p["touched"] for p in deltas), "existing_entities_merged": sum(p["merged"] for p in deltas),
                      "new_entities": sum(p["new_entities"] for p in deltas), "per_piece_seconds": [p["engine_seconds"] for p in deltas]},
            "invariants": delta_invariants(os.path.join(workdir, "trial.db"))}


def format_delta_trial(r: dict) -> str:
    d, b, inv = r["delta"], r["base_load"], r["invariants"]
    st = d["stages"]
    rows = [f"A delta of {r['feed']:,} records onto {r['base']:,} existing people (applied as {len(d['per_piece_seconds'])} deltas; one core)", "",
            f"  base load            {b['seconds']:>8,.0f} s   ({b['records'] / max(b['seconds'], 1e-9):,.0f} records/s, in pieces)",
            f"  THE DELTA            {d['seconds']:>8,.0f} s   ({d['records'] / max(d['seconds'], 1e-9):,.0f} records/s): {d['new']:,} new, {d['changed']:,} changed, {d['unchanged']:,} unchanged",
            f"     classify (key lookups, inside standardize) {st['classify']:>7} s", f"     standardize                                {st['standardize']:>7} s",
            f"     pairs                                      {st['pairs']:>7} s", f"     score                                      {st['score']:>7} s",
            f"     cluster                                    {st['cluster']:>7} s", f"     write                                      {st['write']:>7} s",
            f"  pairs compared {d['pairs']:,} ({d['pairs'] / max(d['records'], 1):.2f} per feed record): {d['auto']:,} auto-merged, {d['review']:,} review",
            f"  existing entities rebuilt {d['existing_entities_touched']:,}, merged into an older one {d['existing_entities_merged']:,}; new entities {d['new_entities']:,}",
            f"  per-delta seconds: {d['per_piece_seconds']}", "",
            f"  afterwards: {inv['records']:,} records, {inv['active_entities']:,} active entities ({inv['merged_entities']:,} merged tombstones), {inv['history_rows']:,} history rows, "
            f"{inv['records_at_version_2']:,} records at version 2, database {inv['database_bytes'] / 1e9:.2f} GB",
            f"  problems: {inv['records_without_an_entity']} records without an entity, {inv['records_not_finished']} unfinished, {inv['records_in_a_merged_entity']} in a merged entity, "
            f"{inv['active_entities_without_records']} active entities with no records"]
    return "\n".join(rows)
