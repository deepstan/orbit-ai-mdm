"""
Customer-editable domain configuration.

A domain (Customer, Product, Vendor, or one you define) is described by a
config FILE instead of Python code: its fields, how each is standardized,
which fields identify a record exactly, how fuzzy signals are weighted,
how candidates are blocked, and how conflicting values are resolved.

    {
      "name": "location",
      "fields": {
        "location_code": "identifier",
        "name":          "name",
        "address_line1": "address_line",
        "postal_code":   "postal_code",
        "country":       {"normalize": ["text", "upper"], "default": "US"}
      },
      "deterministic_fields": ["location_code"],
      "fuzzy_weights": {"name": 0.3, "address_line1": 0.4, "postal_code": 0.3},
      "semantic": {"fields": ["name"], "weight": 0.2},
      "blocking": [["location_code"], ["postal_code", "name"]],
      "survivorship": {"default": "most_recent", "fields": {"name": "most_complete"}}
    }

Adding a domain adds ONE table, `entity_<name>`, with a real column per field (app/golden.py), plus rows in the shared
tables; it needs no change to code. The table is created by `domains_cli.py reindex <name>` (or, where the app is allowed
to run DDL, on first use). The validator below catches most config mistakes up front.

What changing a config means for already-indexed data is handled in
app/indexing.py: edits that affect candidate lookup (blocking, or the
standardization of a blocking field) make the stored index stale, and
ingestion into that domain pauses until `domains_cli.py reindex` runs.
Edits that only affect scoring or survivorship take effect immediately.

Config is deployment-wide, not per-tenant: every tenant in a deployment
shares one definition of each domain. (Per-tenant overrides would mean
per-tenant blocking indexes; not built.)
"""
from __future__ import annotations

import datetime as _datetime
import difflib
import json
import os
import re
from pathlib import Path

from . import domains as _domains
from .domains import BUILTIN_DOMAINS, DomainConfig, register_domain
from .normalizers import freeze_options, validate_field_options
from .normalizers import (ANONYMOUS_KINDS, KEY_KINDS, NORMALIZER_DOCS, NORMALIZERS, PRESENTERS, DeclarativeStandardizer,
                          FieldSpec, KeySpec)
from .fieldtypes import FIELD_TYPES, cast_value
from .golden import RESERVED_FIELD_NAMES
from .survivorship import DEFAULT_STRATEGY

STRATEGIES = ("most_recent", "most_complete", "source_priority")
RESERVED_NAMES = {"admin", "domains"}  # would collide with /v1/admin/... and /v1/domains
SUPPORTED_SUFFIXES = (".json", ".yaml", ".yml")
MAX_FIELDS = 100
MAX_BLOCKING_GROUPS = 20

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
_FIELD_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")  # 63 chars: golden records make every field a column
_TOP_KEYS = {"name", "description", "override", "fields", "deterministic_fields", "exclusive_fields",
             "fuzzy_weights", "semantic", "blocking", "mismatch_penalties", "max_bucket_size", "survivorship"}


class DomainConfigError(Exception):
    """A config file (or set of files) is invalid. The message lists every problem found."""


def _suggest(word, options) -> str:
    match = difflib.get_close_matches(str(word), [str(o) for o in options], n=1)
    return f" Did you mean '{match[0]}'?" if match else ""


def _is_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


# ---------------------------------------------------------------------------
# Parsing + validation
# ---------------------------------------------------------------------------
def parse_domain_document(doc, source: str = "<memory>") -> DomainConfig:
    """Validates one config document and builds a DomainConfig. Collects
    EVERY problem before raising, so fixing a file is one round trip."""
    if not isinstance(doc, dict):
        raise DomainConfigError(f"{source}: the top level must be an object, got {type(doc).__name__}")

    errors: list = []

    def err(path, msg):
        errors.append(f"{path}: {msg}")

    for key in doc:
        if key not in _TOP_KEYS:
            err(key, f"unknown key.{_suggest(key, _TOP_KEYS)} Allowed keys: {', '.join(sorted(_TOP_KEYS))}")

    # --- name / description / override ---
    name = doc.get("name")
    if not isinstance(name, str) or not _NAME_RE.match(name):
        err("name", "required; lowercase letters, digits and underscores, starting with a letter (max 40 chars)")
        name = None
    elif name in RESERVED_NAMES:
        err("name", f"'{name}' is reserved (it would collide with an API route)")
    description = doc.get("description", "")
    if not isinstance(description, str):
        err("description", "must be a string")
        description = ""
    override = doc.get("override", False)
    if not isinstance(override, bool):
        err("override", "must be true or false")
        override = False

    # --- fields: how each is standardized ---
    fields_doc = doc.get("fields")
    specs: dict = {}
    known: set = set()
    if not isinstance(fields_doc, dict) or not fields_doc:
        err("fields", "required: an object mapping each field name to how it is standardized")
    else:
        if len(fields_doc) > MAX_FIELDS:
            err("fields", f"at most {MAX_FIELDS} fields")
        for fname, spec in fields_doc.items():
            path = f"fields.{fname}"
            if not isinstance(fname, str) or not _FIELD_RE.match(fname):
                err(path, "field names must be strings: lowercase letters, digits and underscores, "
                          "starting with a letter (in YAML, quote names like 'no' or 'on')")
                continue
            if fname in RESERVED_FIELD_NAMES:
                err(path, f"'{fname}' is a system column of the golden-record table; choose another field name "
                          f"(reserved: {', '.join(sorted(RESERVED_FIELD_NAMES))})")
                continue
            known.add(fname)
            steps, default, anon_values, anon_defaults, sources, present, ftype, formats = None, None, [], False, (), (), "text", ()
            parse, options, protect = (), {}, ""
            if isinstance(spec, str):
                steps = [spec]
            elif isinstance(spec, list):
                steps = spec
            elif isinstance(spec, dict):
                extra = set(spec) - {"normalize", "default", "anonymous", "anonymous_defaults", "sources", "present", "type", "format", "parse", "options", "protect"}
                if extra:
                    err(path, f"unknown key(s) {sorted(extra)}; allowed: normalize, default, anonymous, anonymous_defaults, sources, present, type, format, parse, options, protect")
                av = spec.get("anonymous", [])
                if not isinstance(av, list) or not all(isinstance(x, str) for x in av):
                    err(f"{path}.anonymous", "must be a list of placeholder strings")
                else:
                    anon_values = av
                ad = spec.get("anonymous_defaults", False)
                if not isinstance(ad, bool):
                    err(f"{path}.anonymous_defaults", "must be true or false")
                else:
                    anon_defaults = ad
                sv = spec.get("sources", [])
                if not isinstance(sv, list) or not all(isinstance(x, str) and _FIELD_RE.match(x) for x in sv):
                    err(f"{path}.sources", "must be a list of raw field names")
                else:
                    sources = tuple(sv)
                pv = spec.get("present", [])
                pv = [pv] if isinstance(pv, str) else pv
                if not isinstance(pv, list) or not all(isinstance(x, str) for x in pv):
                    err(f"{path}.present", "must be a presenter name or a list of names")
                else:
                    unknown = [x for x in pv if x not in PRESENTERS]
                    for x in unknown:
                        err(f"{path}.present", f"unknown presenter '{x}'.{_suggest(x, PRESENTERS)} Available: {', '.join(PRESENTERS)}")
                    if not unknown:
                        present = tuple(pv)
                tv = spec.get("type", "text")
                if tv not in FIELD_TYPES:
                    err(f"{path}.type", f"must be one of {', '.join(FIELD_TYPES)}")
                else:
                    ftype = tv
                fv = spec.get("format", [])
                fv = [fv] if isinstance(fv, str) else fv
                if not isinstance(fv, list) or not all(isinstance(x, str) and "%" in x for x in fv):
                    err(f"{path}.format", "must be a strptime pattern such as \"%m/%d/%Y\", or a list of them")
                elif fv and ftype not in ("date", "timestamp"):
                    err(f"{path}.format", "only applies to date and timestamp fields")
                else:
                    try:
                        for pattern in fv:  # strftime alone accepts nonsense like %Q on some platforms, so prove the pattern round-trips
                            _datetime.datetime.strptime(_datetime.datetime(2026, 1, 2, 3, 4, 5).strftime(pattern), pattern)
                        formats = tuple(fv)
                    except ValueError as e:
                        err(f"{path}.format", f"not a valid strptime pattern: {e}")
                pv2 = spec.get("parse", [])
                pv2 = [pv2] if isinstance(pv2, str) else pv2
                if not isinstance(pv2, list) or not all(isinstance(x, str) and "%" in x for x in pv2):
                    err(f"{path}.parse", "must be a strptime pattern such as \"%m/%d/%Y\", or a list of them")
                else:
                    try:
                        for pattern in pv2:
                            _datetime.datetime.strptime(_datetime.datetime(2026, 1, 2).strftime(pattern), pattern)
                        parse = tuple(pv2)
                    except ValueError as e:
                        err(f"{path}.parse", f"not a valid strptime pattern: {e}")
                options = spec.get("options", {})
                if not isinstance(options, dict):
                    err(f"{path}.options", "must be an object such as {\"itin\": \"allow\"}")
                    options = {}
                pr = spec.get("protect", "")
                if pr not in ("", "hash"):
                    err(f"{path}.protect", "must be \"hash\": the matching form becomes a keyed hash and the cleansed form a mask (see app/protect.py)")
                else:
                    protect = pr
                n = spec.get("normalize", "text")
                steps = [n] if isinstance(n, str) else n
                default = spec.get("default")
                if default is not None and not isinstance(default, str):
                    err(f"{path}.default", "must be a string")
                    default = None
            else:
                err(path, "must be a normalizer name, a list of names, or an object with 'normalize'/'default'")
            if steps is None:
                continue
            if not isinstance(steps, list) or not steps or not all(isinstance(s, str) for s in steps):
                err(f"{path}.normalize", "must be a normalizer name or a non-empty list of names")
                continue
            bad = [s for s in steps if s not in NORMALIZERS]
            for s in bad:
                err(f"{path}.normalize", f"unknown normalizer '{s}'.{_suggest(s, NORMALIZERS)} "
                                         f"Available: {', '.join(NORMALIZERS)}")
            if not bad:
                if anon_defaults and steps[-1] not in ANONYMOUS_KINDS:
                    err(f"{path}.anonymous_defaults",
                        f"there are no built-in placeholders for a '{steps[-1]}' field (available for: {', '.join(ANONYMOUS_KINDS)}); "
                        f"list them in 'anonymous' instead")
                    anon_defaults = False
                # placeholders are compared AFTER standardization, so write them however you like
                for problem in validate_field_options(steps, parse, options):
                    err(f"{path}.options", problem)
                frozen = freeze_options(options)
                anonymous = tuple(sorted({v for v in (FieldSpec(tuple(steps), parse=parse, options=frozen).apply(x) for x in anon_values) if v}))
                if default is not None and ftype != "text":
                    _, why = cast_value(default, ftype, formats)
                    if why:
                        err(f"{path}.default", f"the default {default!r} is not a valid {ftype}: {why}")
                specs[fname] = FieldSpec(tuple(steps), default, anonymous, anon_defaults, sources, present, ftype, formats, parse, frozen, protect)

    def field_list(path, value, allow_empty=True):
        """A list of known, unique field names; returns the valid ones."""
        if value is None:
            return []
        if not isinstance(value, list):
            err(path, "must be a list of field names")
            return []
        out = []
        for f in value:
            if not isinstance(f, str):
                err(path, f"field names must be strings, got {f!r}")
            elif known and f not in known:
                err(path, f"unknown field '{f}'.{_suggest(f, known)}")
            elif f in out:
                err(path, f"'{f}' is listed twice")
            else:
                out.append(f)
        if not allow_empty and not out:
            err(path, "must not be empty")
        return out

    deterministic = field_list("deterministic_fields", doc.get("deterministic_fields"))
    exclusive = field_list("exclusive_fields", doc.get("exclusive_fields"))
    for f in exclusive:
        if f not in deterministic:
            err("exclusive_fields", f"'{f}' must also be listed in deterministic_fields")

    # --- evidence against a match ---
    penalties, exact_fields, date_fields = {}, [], []
    mp = doc.get("mismatch_penalties")
    if mp is not None:
        if not isinstance(mp, dict):
            err("mismatch_penalties", "must be an object mapping field name to a penalty")
        else:
            for f, v in mp.items():
                path = f"mismatch_penalties.{f}"
                if known and f not in known:
                    err(path, f"unknown field.{_suggest(f, known)}")
                    continue
                if isinstance(v, dict):
                    extra = set(v) - {"penalty", "compare"}
                    if extra:
                        err(path, f"unknown key(s) {sorted(extra)}; allowed: penalty, compare")
                    pen, how = v.get("penalty"), v.get("compare", "similarity")
                    if how not in ("exact", "similarity", "date"):
                        err(f"{path}.compare", "must be 'exact', 'similarity' or 'date'")
                        continue
                else:
                    pen, how = v, "similarity"
                if not _is_number(pen) or not 0 < pen <= 1:
                    err(path, "the penalty must be a number greater than 0 and at most 1")
                    continue
                penalties[f] = float(pen)
                if how == "exact":
                    exact_fields.append(f)
                elif how == "date":
                    date_fields.append(f)  # agree / nearly agree (swapped day and month, year one out, a bare year) / disagree

    max_bucket = doc.get("max_bucket_size", 500)
    if isinstance(max_bucket, bool) or not isinstance(max_bucket, int) or not 10 <= max_bucket <= 1_000_000:
        err("max_bucket_size", "must be an integer between 10 and 1,000,000")
        max_bucket = 500

    # --- fuzzy weights ---
    fuzzy: dict = {}
    fw = doc.get("fuzzy_weights")
    if fw is not None:
        if not isinstance(fw, dict):
            err("fuzzy_weights", "must be an object mapping field name to a positive number")
        else:
            for f, w in fw.items():
                if known and f not in known:
                    err(f"fuzzy_weights.{f}", f"unknown field.{_suggest(f, known)}")
                elif not _is_number(w) or w <= 0:
                    err(f"fuzzy_weights.{f}", "must be a number greater than 0")
                else:
                    fuzzy[f] = float(w)

    # --- semantic similarity ---
    sem_fields, sem_weight = [], 0.0
    sem = doc.get("semantic")
    if sem is not None:
        if not isinstance(sem, dict):
            err("semantic", "must be an object with 'fields' and 'weight'")
        else:
            extra = set(sem) - {"fields", "weight"}
            if extra:
                err("semantic", f"unknown key(s) {sorted(extra)}; allowed: fields, weight")
            sem_fields = field_list("semantic.fields", sem.get("fields"))
            w = sem.get("weight")
            if w is None:
                if sem_fields:
                    err("semantic.weight", "required when semantic.fields is set")
            elif not _is_number(w) or w < 0:
                err("semantic.weight", "must be a number, 0 or greater")
            else:
                sem_weight = float(w)

    if not (deterministic or fuzzy or sem_fields):
        err("deterministic_fields / fuzzy_weights / semantic",
            "at least one scoring signal is required, otherwise no two records can ever match")

    # --- blocking: how candidates are found ---
    groups: list = []
    keyspecs: list = []
    unless_groups: list = []
    blocking = doc.get("blocking")
    if not isinstance(blocking, list) or not blocking:
        err("blocking", "required: a non-empty list of field groups, e.g. [[\"email\"], [\"last_name\", \"postal_code\"]]. "
                        "Without blocking, no candidates are ever found")
    else:
        if len(blocking) > MAX_BLOCKING_GROUPS:
            err("blocking", f"at most {MAX_BLOCKING_GROUPS} groups")
        for i, group in enumerate(blocking):
            gpath = f"blocking[{i}]"
            unless = ()
            if isinstance(group, dict):  # {"fields": [...], "unless_any": [...]}: a conditional group
                extra = set(group) - {"fields", "unless_any"}
                if extra:
                    err(gpath, f"unknown key(s) {sorted(extra)}; allowed: fields, unless_any")
                ua = group.get("unless_any", [])
                if not isinstance(ua, list) or not all(isinstance(x, str) for x in ua) or (known and any(x not in known for x in ua)):
                    err(f"{gpath}.unless_any", f"must be a list of field names{'' if not known else ' of this domain'}")
                else:
                    unless = tuple(ua)
                group = group.get("fields")
            if not isinstance(group, list) or not group:
                err(gpath, "must be a non-empty list of fields")
                continue
            members, seen = [], set()
            for j, el in enumerate(group):
                epath = f"{gpath}[{j}]"
                if isinstance(el, str):
                    fname, kind, size = el, "auto", None
                elif isinstance(el, dict):
                    extra = set(el) - {"field", "as", "size"}
                    if extra:
                        err(epath, f"unknown key(s) {sorted(extra)}; allowed: field, as, size")
                    fname, kind, size = el.get("field"), el.get("as", "auto"), el.get("size")
                else:
                    err(epath, "must be a field name, or an object like {\"field\": \"last_name\", \"as\": \"soundex\"}")
                    continue
                if not isinstance(fname, str) or (known and fname not in known):
                    err(epath, f"unknown field {fname!r}.{_suggest(fname, known)}")
                    continue
                if fname in seen:
                    err(epath, f"'{fname}' appears twice in this group")
                    continue
                if kind not in KEY_KINDS:
                    err(f"{epath}.as", f"must be one of {', '.join(KEY_KINDS)}")
                    continue
                if size is not None and (isinstance(size, bool) or not isinstance(size, int) or not 1 <= size <= 12
                                         or kind not in ("prefix", "windows")):
                    err(f"{epath}.size", "must be an integer from 1 to 12, and only applies to 'prefix' and 'windows'")
                    continue
                seen.add(fname)
                members.append(KeySpec(fname, kind, size))
            if len(members) == len(group):  # every element was valid
                if any(members == ks and unless == un for ks, un in zip(keyspecs, unless_groups)):
                    err(gpath, "duplicate of an earlier group")
                else:
                    keyspecs.append(members)
                    groups.append([m.field for m in members])
                    unless_groups.append(unless)

    warnings = [
        f"deterministic field '{f}' has no blocking group of its own, so two records that share only a {f} are "
        f"never compared, and never matched on it"
        for f in deterministic if not any(len(g) == 1 and g[0].field == f for g in keyspecs)
    ] if not errors else []

    # --- survivorship ---
    default_strategy, strategies = DEFAULT_STRATEGY, {}
    surv = doc.get("survivorship")
    if surv is not None:
        if not isinstance(surv, dict):
            err("survivorship", "must be an object with 'default' and/or 'fields'")
        else:
            extra = set(surv) - {"default", "fields"}
            if extra:
                err("survivorship", f"unknown key(s) {sorted(extra)}; allowed: default, fields")
            d = surv.get("default", DEFAULT_STRATEGY)
            if d not in STRATEGIES:
                err("survivorship.default", f"must be one of {', '.join(STRATEGIES)}")
            else:
                default_strategy = d
            sf = surv.get("fields", {})
            if not isinstance(sf, dict):
                err("survivorship.fields", "must be an object mapping field name to a strategy")
            else:
                for f, s in sf.items():
                    if known and f not in known:
                        err(f"survivorship.fields.{f}", f"unknown field.{_suggest(f, known)}")
                    elif s not in STRATEGIES:
                        err(f"survivorship.fields.{f}", f"must be one of {', '.join(STRATEGIES)}")
                    else:
                        strategies[f] = s

    for f, sp in specs.items():  # a hashed field can only ever be compared for EQUALITY: anything else would act on the hash, not the value
        if not getattr(sp, "protect", ""):
            continue
        if f in fuzzy:
            err(f"fuzzy_weights.{f}", f"'{f}' is protected (hashed), so it can only be compared exactly: remove it from fuzzy_weights")
        if f in sem_fields:
            err("semantic.fields", f"'{f}' is protected (hashed) and cannot feed the semantic signal")
        for group in keyspecs:
            for ks in group:
                if ks.field == f and ks.kind != "exact":
                    err("blocking", f"'{f}' is protected (hashed): block on it only as {{\"field\": \"{f}\", \"as\": \"exact\"}}, not '{ks.kind}'")
        if f in penalties and f not in deterministic and f not in exact_fields:
            err(f"mismatch_penalties.{f}", f"'{f}' is protected (hashed): list it in deterministic_fields (or mismatch_exact) so it is compared exactly")

    for f in date_fields:
        if f in specs and specs[f].type != "date":
            err(f"mismatch_penalties.{f}.compare", f"'date' comparison needs a field of type date; '{f}' is {specs[f].type}")
    for f, sp in specs.items():
        if sp.opt("near") and not sp.protect:
            err(f"fields.{f}.options.near", "needs \"protect\": \"hash\": near-equality works on hashes of the value's typo variants")

    if errors:
        raise DomainConfigError(
            f"Invalid domain config {source} ({len(errors)} problem{'s' if len(errors) != 1 else ''}):\n  - "
            + "\n  - ".join(errors))

    schema_fields = list(specs)
    return DomainConfig(
        name=name,
        schema_fields=schema_fields,
        standardizer=DeclarativeStandardizer(schema_fields, specs),
        deterministic_fields=deterministic,
        fuzzy_weights=fuzzy,
        semantic_fields=sem_fields,
        semantic_weight=sem_weight,
        blocking_field_groups=groups,
        field_strategies=strategies,
        default_strategy=default_strategy,
        field_specs=specs,
        source=source,
        description=description,
        override=override,
        mismatch_penalties=penalties,
        mismatch_date=date_fields,
        blocking_unless=unless_groups,
        mismatch_exact=exact_fields,
        exclusive_fields=exclusive,
        max_bucket_size=max_bucket,
        blocking_keyspecs=keyspecs if any(m.kind != "auto" or m.size is not None for g in keyspecs for m in g) else [],
        warnings=warnings,
    )


def config_to_document(config: DomainConfig) -> dict:
    """The inverse of parse_domain_document(): a config as an editable document. `export` uses this so a
    built-in domain can be dumped, tweaked, and loaded back as an override."""
    doc: dict = {"name": config.name}
    if config.description:
        doc["description"] = config.description
    if config.name in BUILTIN_DOMAINS:
        doc["override"] = True
    doc["fields"] = {}
    for f in config.schema_fields:
        spec = config.field_specs[f]
        entry = {"normalize": list(spec.normalize)}
        if spec.default is not None:
            entry["default"] = spec.default
        if spec.anonymous_defaults:
            entry["anonymous_defaults"] = True
        if spec.anonymous:
            entry["anonymous"] = sorted(spec.anonymous)
        if spec.sources:
            entry["sources"] = list(spec.sources)
        if spec.present:
            entry["present"] = list(spec.present)
        if spec.type != "text":
            entry["type"] = spec.type
        if spec.formats:
            entry["format"] = list(spec.formats)
        if spec.parse:
            entry["parse"] = list(spec.parse)
        if spec.options:
            entry["options"] = {k: (list(v) if isinstance(v, tuple) else v) for k, v in spec.options}
        if spec.protect:
            entry["protect"] = spec.protect
        doc["fields"][f] = entry
    doc["deterministic_fields"] = list(config.deterministic_fields)
    if config.exclusive_fields:
        doc["exclusive_fields"] = list(config.exclusive_fields)
    doc["fuzzy_weights"] = dict(config.fuzzy_weights)
    doc["semantic"] = {"fields": list(config.semantic_fields), "weight": config.semantic_weight}
    doc["blocking"] = [({"fields": [spec.to_json() for spec in group], "unless_any": list(config.blocking_unless[i])}
                        if i < len(config.blocking_unless) and config.blocking_unless[i] else [spec.to_json() for spec in group])
                       for i, group in enumerate(config.keyspecs())]
    if config.mismatch_penalties:
        doc["mismatch_penalties"] = {
            f: ({"penalty": p, "compare": "exact"} if f in config.mismatch_exact and f not in config.deterministic_fields
                else {"penalty": p, "compare": "date"} if f in config.mismatch_date else p)
            for f, p in config.mismatch_penalties.items()}
    doc["max_bucket_size"] = config.max_bucket_size
    doc["survivorship"] = {"default": config.default_strategy, "fields": dict(config.field_strategies)}
    return doc


# ---------------------------------------------------------------------------
# Files and directories
# ---------------------------------------------------------------------------
def load_domain_file(path) -> DomainConfig:
    path = Path(path)
    suffix = path.suffix.lower()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise DomainConfigError(f"{path}: cannot read file: {e}") from e
    if suffix == ".json":
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as e:
            raise DomainConfigError(f"{path}: not valid JSON: {e}") from e
    elif suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as e:
            raise DomainConfigError(f"{path}: YAML config needs PyYAML (pip install pyyaml), or use a .json file") from e
        try:
            doc = yaml.safe_load(text)  # safe_load: a config file must never be able to construct arbitrary objects
        except yaml.YAMLError as e:
            raise DomainConfigError(f"{path}: not valid YAML: {e}") from e
    else:
        raise DomainConfigError(f"{path}: unsupported file type '{suffix}' (use .json, .yaml or .yml)")
    return parse_domain_document(doc, source=str(path))


def load_domains_dir(directory) -> list:
    """Loads every config file in a directory. Reports ALL broken files at
    once, and rejects two files defining the same domain."""
    d = Path(directory)
    files = sorted(p for p in d.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_SUFFIXES)
    configs, problems, seen = [], [], {}
    for f in files:
        try:
            cfg = load_domain_file(f)
        except DomainConfigError as e:
            problems.append(str(e))
            continue
        if cfg.name in seen:
            problems.append(f"{f}: domain '{cfg.name}' is already defined in {seen[cfg.name]}")
            continue
        seen[cfg.name] = f
        configs.append(cfg)
    if problems:
        raise DomainConfigError("\n\n".join(problems))
    return configs


def check_applicable(configs) -> list:
    """Rules about how configs relate to the built-in domains. Returns problems (empty if fine)."""
    problems = []
    for c in configs:
        if c.name in BUILTIN_DOMAINS and not c.override:
            problems.append(
                f"{c.source}: '{c.name}' is a built-in domain. To change it, add \"override\": true. "
                f"(If you change its blocking or the standardization of a blocking field, "
                f"you will also need to run a reindex.)")
        elif c.name not in BUILTIN_DOMAINS and c.override:
            problems.append(
                f"{c.source}: \"override\": true, but '{c.name}' is not a built-in domain "
                f"({', '.join(BUILTIN_DOMAINS)}). Remove it, or fix the name.")
    return problems


def apply_configs(configs) -> None:
    problems = check_applicable(configs)
    if problems:
        raise DomainConfigError("\n\n".join(problems))
    for c in configs:
        try:
            register_domain(c, replace=c.name in BUILTIN_DOMAINS)
        except KeyError as e:
            raise DomainConfigError(f"{c.source}: {e.args[0]}") from e


_loaded: dict = {}  # resolved directory -> names loaded, so repeated calls are no-ops


def load_configured_domains(directory=None) -> list:
    """Loads customer domain config into the registry. Call once at
    startup from every entry point (the API and each CLI do). The
    directory is, in order: the argument, $MDM_DOMAINS_DIR, ./domains.

    A missing ./domains is fine (no customization). A missing directory
    that was asked for explicitly is an error, and so is any invalid file:
    starting with a half-applied domain set would be worse than not starting."""
    explicit = directory or os.environ.get("MDM_DOMAINS_DIR")
    path = Path(explicit) if explicit else Path("domains")
    if not path.is_dir():
        if explicit:
            raise DomainConfigError(f"Domain config directory '{path}' does not exist")
        return []
    key = str(path.resolve())
    if key in _loaded:
        return _loaded[key]
    configs = load_domains_dir(path)
    apply_configs(configs)
    _loaded[key] = [c.name for c in configs]
    return _loaded[key]


def reset() -> None:
    """Back to the built-in domains only. For tests."""
    _domains.reset_registry()
    _loaded.clear()
