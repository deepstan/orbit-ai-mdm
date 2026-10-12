"""
Profiling: find placeholder values that no rule can know about, before they merge strangers.

The cleansing rules (app/normalizers.py: ssn, dob) catch STRUCTURAL placeholders: an SSN of 000-00-0000, a DOB of 1900-01-01.
But every source system has defaults of its own (an old CRM that stamps 1960-06-15 on every record nobody filled in, a
migration that wrote the same SSN on thousands of rows). Those look valid. They are recognisable by what they DO: one value
shared by many records whose NAMES disagree. A real SSN belongs to one person; a real date of birth is shared by a modest,
predictable number of people.

`profile_records` finds those values in one streaming pass, so it is the step to run when a new source system is onboarded,
on the file BEFORE it is loaded or on loaded data. It reports what the rules already reject (with reasons), what share of a
date field sits on January 1st (real data: about 0.3%; defaulted data: far more), and the suspect values. Nothing is changed:
a person reviews the suspects and, if they agree, pastes the suggested `anonymous` list into the field's config. From then on
it is an ordinary, deterministic, versioned rule.

SSNs are masked in the report (***-**-6789) unless `reveal=True`; only values shared by many different people are listed at
all, so an ordinary person's SSN or DOB never appears. Memory is bounded: when a field has more than `max_distinct` distinct
values, those seen once are dropped (`approximate` says so); a placeholder frequent enough to matter always survives.
"""
from __future__ import annotations

import math
from collections import Counter

from . import protect
from .domains import get_domain_config

NAME_SAMPLE = 40  # per value, how many records' names are remembered (to judge how varied they are)


class _Field:
    def __init__(self):
        self.total = self.missing = self.accepted = self.jan1 = self.year_only = 0
        self.rejected: Counter = Counter()
        self.counts: dict = {}
        self.names: dict = {}
        self.years = [None, None]
        self.pruned = False

    def prune(self):
        self.counts = {v: c for v, c in self.counts.items() if c > 1}
        self.names = {v: n for v, n in self.names.items() if v in self.counts}
        self.pruned = True


def _shown(spec, value: str, reveal: bool) -> str:
    if not reveal and "ssn" in spec.normalize and len(value) >= 4:
        return "***-**-" + value[-4:]
    return value


def _profile_records(domain: str, records, *, fields=None, min_count: int = 20, min_names: int = 5, min_name_ratio: float = 0.5,
                    max_distinct: int = 2_000_000, reveal: bool = False) -> dict:
    """records: an iterable of raw attribute dicts (a file, or the loaded raw layer). A value is SUSPECT when it is shared by at
    least max(min_count, 10 x the average count per distinct value) records AND, in a sample of those records, at least
    `min_names` different names make up at least `min_name_ratio` of the sample. (The first guards against dates, which are
    legitimately shared by many people; the second against one person repeated.)"""
    config = get_domain_config(domain)
    specs = getattr(config.standardizer, "specs", None)
    if not specs:
        raise ValueError(f"domain '{domain}' has no declarative field specs to profile")
    chosen = list(fields) if fields else [f for f in config.schema_fields if specs[f].reports_rejections]
    unknown = [f for f in chosen if f not in specs]
    if unknown:
        raise ValueError(f"unknown field(s) {unknown}; fields of '{domain}': {', '.join(config.schema_fields)}")
    if not chosen:
        raise ValueError(f"domain '{domain}' has no ssn or dob field; name the fields to profile with --field")
    stats = {f: _Field() for f in chosen}
    n = 0
    for raw in records:
        n += 1
        matching, _ = config.standardizer.standardize_both(raw)
        who = "|".join(matching.get(f, "") for f in config.semantic_fields)  # the fields the matcher compares names on
        who = who if who.strip("|") else ""
        for f in chosen:
            spec, st = specs[f], stats[f]
            value = next((raw.get(x) for x in (spec.sources or (f,)) if raw.get(x) not in (None, "")), None)
            st.total += 1
            if value is None or str(value).strip() == "":
                st.missing += 1
                continue
            m = spec.plain(value)  # the real normalized value: a protected field's matching form is a hash, and this tool stores nothing
            if m == "":
                st.rejected[spec.diagnose(value) or "emptied by the field's rules"] += 1
                continue
            st.accepted += 1
            c = st.counts[m] = st.counts.get(m, 0) + 1
            if who and c <= NAME_SAMPLE:
                st.names.setdefault(m, set()).add(who)
            if len(m) == 4:
                st.year_only += 1
            else:
                st.jan1 += m.endswith("-01-01")
            if m[:4].isdigit():
                y = int(m[:4])
                st.years = [y if st.years[0] is None else min(st.years[0], y), y if st.years[1] is None else max(st.years[1], y)]
            if len(st.counts) > max_distinct:
                st.prune()

    out_fields, snippet = {}, {}
    for f, st in stats.items():
        spec = specs[f]
        distinct = len(st.counts)
        mean = st.accepted / distinct if distinct else 0.0
        threshold = max(min_count, math.ceil(10 * mean))
        has_names = any(st.names.values())
        suspects = []
        for value, c in st.counts.items():
            if c < threshold:
                continue
            names = len(st.names.get(value, ()))
            sample = min(c, NAME_SAMPLE)
            if has_names and (names < min_names or names / sample < min_name_ratio):
                continue
            suspects.append({"value": _shown(spec, value, reveal), "count": c, "share": round(c / st.accepted, 4),
                             "different_names_in_sample": names, "sample": sample})
            if reveal:
                snippet.setdefault(f, {"anonymous": []})["anonymous"].append(value)
        suspects.sort(key=lambda s: -s["count"])
        rej = sum(st.rejected.values())
        out_fields[f] = {"total": st.total, "missing": st.missing, "rejected_by_rules": rej, "rejected_reasons": dict(st.rejected.most_common()),
                         "accepted": st.accepted, "distinct": distinct, "threshold": threshold, "suspects": suspects[:20],
                         "jan1_share": round(st.jan1 / st.accepted, 4) if st.accepted else 0.0,
                         "jan1_elevated": bool(st.accepted >= 200 and st.jan1 / st.accepted > 0.015),
                         "year_only": st.year_only, "year_range": st.years if st.years[0] is not None else None}
    return {"domain": domain, "records": n, "approximate": any(s.pruned for s in stats.values()),
            "settings": {"min_count": min_count, "min_names": min_names, "min_name_ratio": min_name_ratio},
            "fields": out_fields, "config_snippet": {"fields": snippet} if snippet else None}


def profile_records(domain: str, records, **kw) -> dict:
    with protect.unkeyed():  # read-only: profiling never needs the hashing key
        return _profile_records(domain, records, **kw)


def format_profile(report: dict) -> str:
    lines = [f"Profile of '{report['domain']}': {report['records']:,} records" + ("  (approximate: memory cap reached, once-seen values were dropped)" if report["approximate"] else "")]
    for f, r in report["fields"].items():
        pct = lambda x: f"{100 * x / r['total']:.1f}%" if r["total"] else "-"
        lines += ["", f, f"  records {r['total']:,}  |  missing {pct(r['missing'])}  |  rejected by the rules {pct(r['rejected_by_rules'])}  |  accepted {pct(r['accepted'])}"]
        for reason, c in r["rejected_reasons"].items():
            lines.append(f"    rejected: {reason}: {c:,}")
        if r["year_only"]:
            lines.append(f"    year only (kept for matching, not a golden date): {r['year_only']:,}")
        if r["jan1_elevated"]:
            lines.append(f"    {100 * r['jan1_share']:.1f}% of dates are January 1st (real data: about 0.3%): many are probably defaulted")
        if r["suspects"]:
            lines.append(f"  SUSPECT placeholders: shared by at least {r['threshold']:,} records, with many different names")
            for s in r["suspects"]:
                lines.append(f"    {s['value']}  {s['count']:,} records ({100 * s['share']:.1f}% of the field), {s['different_names_in_sample']} different names in a sample of {s['sample']}")
        else:
            lines.append("  no suspect placeholder values")
    if report["config_snippet"]:
        import json
        lines += ["", "To treat the suspects as missing, review them, then add to the domain config:", json.dumps(report["config_snippet"], indent=2)]
    elif any(r["suspects"] for r in report["fields"].values()):
        lines += ["", "(values are masked; re-run with --reveal to print a config snippet to paste)"]
    return "\n".join(lines)
