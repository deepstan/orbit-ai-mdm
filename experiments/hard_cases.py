"""
A benchmark of the cases that tell matching designs apart, with ground truth.

The shipped benchmark (app/benchmark_person.py) only has corrupted copies of one base record, so, in its own words, "there are no chains": it cannot show what
comparing a record to an entity (or a golden record) changes. This one has the cases where designs differ:

  background        people who match nobody
  dup_clean         the same person, 2-4 lightly corrupted copies sharing an email or phone
  dup_fuzzy         the same person, copies sharing NO identifier (name + address only)
  multi_ident       one person with two emails and two phones: A-B share an email, B-C a phone, A and C share nothing
  family_shared     DIFFERENT people (a household) sharing a phone and an address
  junior_senior     DIFFERENT people with the same name, address and phone (Jr / Sr)
  shared_generic    DIFFERENT people sharing a generic email or a default phone
  bridge_error      two people, and a record with one person's name and email and the OTHER's phone (a data-entry mix)
  moved             the same person at a new address
  upd_split         two copies of one person; later ONE is updated into a different person (it must leave the group)
  upd_fix           a copy with a typo is corrected by an update (it must stay)
  upd_move          a record's email is updated to another existing person's (it must move groups)
  upd_life          the SAME person's record is updated after a life event: a new surname (marriage), or a new address and phone (a move); email unchanged. It must STAY.

truth is per record AFTER the updates. Everything is pure in the seed.
"""
from __future__ import annotations

import random
from collections import defaultdict

NICK = {"Robert": ["Bob", "Rob", "Bobby"], "William": ["Bill", "Will"], "Elizabeth": ["Liz", "Beth"], "James": ["Jim", "Jimmy"], "Margaret": ["Peggy", "Maggie"],
        "Richard": ["Rick", "Dick"], "Michael": ["Mike"], "Jennifer": ["Jen", "Jenny"], "Katherine": ["Kate", "Katie"], "Thomas": ["Tom"], "Joseph": ["Joe"],
        "Daniel": ["Dan", "Danny"], "Patricia": ["Pat", "Patty"], "Susan": ["Sue"], "David": ["Dave"], "Barbara": ["Barb"], "Charles": ["Chuck", "Charlie"],
        "Steven": ["Steve"], "Andrew": ["Andy"], "Maria": [], "Carlos": [], "Wei": [], "Aisha": [], "Omar": [], "Priya": [], "Ivan": [], "Sofia": [], "Linda": [],
        "Karen": [], "Nancy": [], "Henry": ["Hank"], "Dorothy": ["Dot"], "Frances": ["Fran"], "Gregory": ["Greg"], "Samuel": ["Sam"], "Anthony": ["Tony"]}
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
        "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
        "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
        "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
        "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson"]
STREETS = ["Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Lake", "Hill", "Park", "Main", "Church", "Spring", "Ridge", "River", "Walnut", "Peachtree", "Highland", "Sunset"]
KINDS = [("Street", "St"), ("Avenue", "Ave"), ("Road", "Rd"), ("Drive", "Dr"), ("Lane", "Ln"), ("Boulevard", "Blvd")]
CITIES = [("Atlanta", "GA", "303"), ("Macon", "GA", "312"), ("Savannah", "GA", "314"), ("Athens", "GA", "306"), ("Augusta", "GA", "309"), ("Marietta", "GA", "300"),
          ("Decatur", "GA", "300"), ("Roswell", "GA", "300"), ("Columbus", "GA", "319"), ("Albany", "GA", "317")]
GENERIC_EMAILS = ["info@example.com", "noemail@none.com", "customer@email.com"]
DEFAULT_PHONE = "000-000-0000"


class Builder:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)
        self.n = 0                     # unique-number source for emails and phones
        self.true_id = 0
        self.households = 0
        self.rows: list[dict] = []     # {rid, raw, true, scenario, household}
        self.updates: list[dict] = []  # {rid, raw, true, scenario}

    # ---- people
    def person(self, last=None, address=None, first=None) -> dict:
        r = self.rng
        self.n += 1
        city, state, zp = r.choice(CITIES)
        first = first or r.choice(list(NICK))
        last = last or r.choice(LAST)
        kind_long, _ = r.choice(KINDS)
        address = address or f"{r.randint(10, 9899)} {r.choice(STREETS)} {kind_long}"
        return {"first": first, "last": last, "email": f"{first.lower()}.{last.lower()}{self.n}@mail{r.randint(1, 6)}.example", "phone": f"404-{200 + self.n // 9000:03d}-{self.n % 9000 + 1000:04d}",
                "address": address, "city": city, "state": state, "zip": f"{zp}{r.randint(10, 99)}"}

    def typo(self, s: str) -> str:
        r = self.rng
        if len(s) < 4:
            return s
        i = r.randint(1, len(s) - 2)
        op = r.choice(["swap", "drop", "dup"])
        return s[:i] + s[i + 1] + s[i] + s[i + 2:] if op == "swap" else s[:i] + s[i + 1:] if op == "drop" else s[:i] + s[i] + s[i:]

    def raw(self, p: dict, noise: float = 0.0, keep_email=True, keep_phone=True, name_suffix: str = "") -> dict:
        """One source record of a person. `noise` is the chance of each kind of corruption."""
        r = self.rng
        first, last, addr = p["first"], p["last"], p["address"]
        if r.random() < noise * 1.5 and NICK.get(first):
            first = r.choice(NICK[first])
        if r.random() < noise * 0.6:
            last = self.typo(last)
        if r.random() < noise * 0.5:
            first = self.typo(first)
        if r.random() < noise:
            for long, short in KINDS:
                addr = addr.replace(long, short)
        if r.random() < noise * 0.5:
            addr = ""
        email = p["email"] if keep_email and r.random() > noise * 0.4 else ""
        phone = p["phone"] if keep_phone and r.random() > noise * 0.4 else ""
        if email and r.random() < noise * 0.3:
            email = email.upper()
        out = {"first_name": first, "last_name": last, "email": email, "phone": phone, "address_line1": addr, "city": p["city"], "state": p["state"], "postal_code": p["zip"]}
        if name_suffix:
            out["name_suffix"] = name_suffix
        return out

    # ---- bookkeeping
    def new_entity(self) -> int:
        self.true_id += 1
        return self.true_id

    def new_household(self) -> int:
        self.households += 1
        return self.households

    def add(self, raw: dict, true: int, scenario: str, household: int | None = None) -> str:
        rid = f"r{len(self.rows):05d}"
        self.rows.append({"rid": rid, "raw": raw, "true": true, "scenario": scenario, "household": household})
        return rid

    def update(self, rid: str, raw: dict, true: int):
        self.updates.append({"rid": rid, "raw": raw, "true": true})

    # ---- scenarios
    def background(self, n):
        for _ in range(n):
            self.add(self.raw(self.person(), 0.0), self.new_entity(), "background")

    def dup_clean(self, n):
        for _ in range(n):
            p, t = self.person(), self.new_entity()
            copies = self.rng.randint(2, 4)
            shared = self.rng.choice(["email", "phone", "both"])
            for _ in range(copies):
                self.add(self.raw(p, 0.35, keep_email=shared in ("email", "both"), keep_phone=shared in ("phone", "both")), t, "dup_clean")

    def dup_fuzzy(self, n):
        for _ in range(n):
            p, t = self.person(), self.new_entity()
            for _ in range(2):
                self.add(self.raw(p, 0.25, keep_email=False, keep_phone=False), t, "dup_fuzzy")

    def multi_ident(self, n):
        for _ in range(n):
            p, t = self.person(), self.new_entity()
            self.n += 1
            p2 = {**p, "email": f"alt{self.n}.{p['last'].lower()}@other.example", "phone": f"678-{200 + self.n // 9000:03d}-{self.n % 9000 + 1000:04d}"}
            self.add(self.raw(p, 0.15), t, "multi_ident")                                  # email1 + phone1
            self.add(self.raw({**p, "phone": p2["phone"]}, 0.15), t, "multi_ident")        # email1 + phone2
            self.add(self.raw({**p, "email": p2["email"], "phone": p2["phone"]}, 0.15), t, "multi_ident")   # email2 + phone2

    def family_shared(self, n):
        for _ in range(n):
            base = self.person()
            household = self.new_household()
            members = self.rng.sample(sorted(NICK), 3)
            for first in members:
                self.n += 1
                p = {**base, "first": first, "email": f"{first.lower()}.{base['last'].lower()}{self.n}@mail{self.rng.randint(1, 6)}.example"}
                self.add(self.raw(p, 0.1, keep_email=self.rng.random() < 0.7), self.new_entity(), "family_shared", household)

    def junior_senior(self, n):
        for _ in range(n):
            base = self.person()
            household = self.new_household()
            for suffix in ("Sr", "Jr"):
                self.n += 1
                p = {**base, "email": f"{suffix.lower()}.{base['last'].lower()}{self.n}@mail{self.rng.randint(1, 6)}.example"}
                self.add(self.raw(p, 0.1, keep_email=self.rng.random() < 0.6, name_suffix=suffix), self.new_entity(), "junior_senior", household)

    def shared_generic(self, per_value):
        for g, value in enumerate(GENERIC_EMAILS + [DEFAULT_PHONE]):
            for _ in range(per_value):
                p = self.person()
                raw = self.raw(p, 0.1, keep_email=False, keep_phone=False)
                raw["phone" if value == DEFAULT_PHONE else "email"] = value
                self.add(raw, self.new_entity(), "shared_generic")

    def bridge_error(self, n):
        for _ in range(n):
            x, y = self.person(), self.person()
            tx, ty = self.new_entity(), self.new_entity()
            self.add(self.raw(x, 0.1), tx, "bridge_error")
            self.add(self.raw(y, 0.1), ty, "bridge_error")
            mixed = self.raw({**x, "phone": y["phone"]}, 0.1)       # X's name and email, Y's phone: it is X
            self.add(mixed, tx, "bridge_error")

    def moved(self, n):
        for _ in range(n):
            p, t = self.person(), self.new_entity()
            self.add(self.raw(p, 0.1), t, "moved")
            city, state, zp = self.rng.choice(CITIES)
            self.add(self.raw({**p, "address": f"{self.rng.randint(10, 9899)} {self.rng.choice(STREETS)} Street", "city": city, "state": state, "zip": f"{zp}{self.rng.randint(10, 99)}"}, 0.1, keep_phone=False), t, "moved")

    def upd_split(self, n):
        for _ in range(n):
            p, t = self.person(), self.new_entity()
            a = self.add(self.raw(p, 0.1), t, "upd_split")
            self.add(self.raw(p, 0.1), t, "upd_split")
            other = self.person()                                     # the update turns record A into someone else entirely
            self.update(a, self.raw(other, 0.0), self.new_entity())

    def upd_fix(self, n):
        for _ in range(n):
            p, t = self.person(), self.new_entity()
            self.add(self.raw(p, 0.1), t, "upd_fix")
            bad = self.raw(p, 0.0)
            bad["last_name"] = self.typo(self.typo(p["last"]))
            bad["first_name"] = self.typo(p["first"])
            a = self.add(bad, t, "upd_fix")
            self.update(a, self.raw(p, 0.0), t)

    def upd_move(self, n):
        for _ in range(n):
            q, tq = self.person(), self.new_entity()                   # an existing person with two records
            self.add(self.raw(q, 0.1), tq, "upd_move")
            self.add(self.raw(q, 0.1), tq, "upd_move")
            p, tp = self.person(), self.new_entity()
            a = self.add(self.raw(p, 0.1, keep_phone=False), tp, "upd_move")        # A is first someone else ...
            self.update(a, self.raw(q, 0.1, keep_phone=False), tq)                  # ... then corrected to be q: it must move


    def upd_life(self, n):
        for i in range(n):
            p, t = self.person(), self.new_entity()
            a = self.add(self.raw(p, 0.1), t, "upd_life")
            self.add(self.raw(p, 0.1), t, "upd_life")
            if i % 2 == 0:                                               # marriage: a new surname, the email and phone unchanged
                changed = {**p, "last": self.rng.choice([x for x in LAST if x != p["last"]])}
            else:                                                        # a move: a new address and phone, the name and email unchanged
                city, state, zp = self.rng.choice(CITIES)
                self.n += 1
                changed = {**p, "address": f"{self.rng.randint(10, 9899)} {self.rng.choice(STREETS)} Street", "city": city, "state": state, "zip": f"{zp}{self.rng.randint(10, 99)}",
                           "phone": f"770-{200 + self.n // 9000:03d}-{self.n % 9000 + 1000:04d}"}
            self.update(a, self.raw(changed, 0.0), t)                    # truth: still the same person


def build(seed: int = 1, scale: float = 1.0, life_events: bool = False) -> dict:
    b = Builder(seed)
    s = lambda k: max(1, int(k * scale))
    b.background(s(300)); b.dup_clean(s(80)); b.dup_fuzzy(s(40)); b.multi_ident(s(40)); b.family_shared(s(40)); b.junior_senior(s(30))
    b.shared_generic(s(8)); b.bridge_error(s(20)); b.moved(s(30)); b.upd_split(s(30)); b.upd_fix(s(20)); b.upd_move(s(20))
    if life_events:                                                      # appended LAST and off by default: the earlier results stay comparable
        b.upd_life(s(40))
    final_true = {r["rid"]: r["true"] for r in b.rows}
    for u in b.updates:
        final_true[u["rid"]] = u["true"]
    return {"rows": b.rows, "updates": b.updates, "truth": final_true, "scenario": {r["rid"]: r["scenario"] for r in b.rows},
            "household": {r["rid"]: r["household"] for r in b.rows if r["household"] is not None}}


# ------------------------------------------------------------------------------------------------ metrics
def _pairs(groups: dict) -> set:
    out = set()
    for members in groups.values():
        ms = sorted(members)
        for i in range(len(ms)):
            for j in range(i + 1, len(ms)):
                out.add((ms[i], ms[j]))
    return out


def pairs_of(assign: dict) -> set:
    g = defaultdict(list)
    for rid, c in assign.items():
        g[c].append(rid)
    return _pairs(g)


def metrics(assign: dict, data: dict) -> dict:
    """Pairwise precision / recall / F1 over the final state, overall and per scenario (a pair across scenarios is 'cross')."""
    pred, true = pairs_of(assign), pairs_of(data["truth"])
    scen = data["scenario"]
    label = lambda p: scen[p[0]] if scen[p[0]] == scen[p[1]] else "cross"
    stats = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})
    for p in pred & true: stats[label(p)]["tp"] += 1
    for p in pred - true: stats[label(p)]["fp"] += 1
    for p in true - pred: stats[label(p)]["fn"] += 1
    tp, fp, fn = len(pred & true), len(pred - true), len(true - pred)
    f = lambda tp, fp, fn: (tp / (tp + fp) if tp + fp else 1.0, tp / (tp + fn) if tp + fn else 1.0)
    p, r = f(tp, fp, fn)
    return {"precision": p, "recall": r, "f1": 2 * p * r / (p + r) if p + r else 0.0, "tp": tp, "fp": fp, "fn": fn, "by_scenario": dict(stats)}


def disagreement(assign_a: dict, assign_b: dict) -> int:
    """How many record pairs are together in one run and apart in the other: the order sensitivity of a system."""
    return len(pairs_of(assign_a) ^ pairs_of(assign_b))


def household_metrics(links: set, assign: dict, data: dict) -> dict:
    """How well LINKS between groups find the households: recall = of the pairs of different people who share a household, how many have a link between
    (a group of) the one and (a group of) the other; precision = of the links, how many join groups that really do hold two people of one household."""
    by_house = defaultdict(lambda: defaultdict(list))
    for rid, hid in data["household"].items():
        by_house[hid][data["truth"][rid]].append(rid)
    groups_of = lambda recs: {assign[r] for r in recs}
    wanted = found = 0
    for people in by_house.values():
        ids = sorted(people)
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                wanted += 1
                ga, gb = groups_of(people[ids[i]]), groups_of(people[ids[j]])
                found += any(frozenset((a, b)) in links for a in ga for b in gb if a != b)
    members = defaultdict(list)
    for rid, g in assign.items():
        members[g].append(rid)
    good = 0
    for link in links:
        a, b = tuple(link)
        good += any(data["household"].get(r1) is not None and data["household"].get(r1) == data["household"].get(r2) and data["truth"][r1] != data["truth"][r2]
                    for r1 in members[a] for r2 in members[b])
    return {"links": len(links), "good_links": good, "pairs": wanted, "found": found,
            "precision": good / len(links) if links else 1.0, "recall": found / wanted if wanted else 1.0}
