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

Adding a domain needs NO database change: records are stored as JSON, and
every table is keyed by a `domain` string. That is why this is safe to hand
to customers -- the worst a bad config can do is match badly, and the
validator below exists to catch most of those mistakes up front.

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

import difflib
import json
import os
import re
from pathlib import Path

from . import domains as _domains
from .domains import BUILTIN_DOMAINS, DomainConfig, register_domain
from .normalizers import NORMALIZER_DOCS, NORMALIZERS, DeclarativeStandardizer, FieldSpec
from .survivorship import DEFAULT_STRATEGY

STRATEGIES = ("most_recent", "most_complete", "source_priority")
RESERVED_NAMES = {"admin", "domains"}  # would collide with /v1/admin/... and /v1/domains
SUPPORTED_SUFFIXES = (".json", ".yaml", ".yml")
MAX_FIELDS = 100
MAX_BLOCKING_GROUPS = 20

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
_FIELD_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_TOP_KEYS = {"name", "description", "override", "fields", "deterministic_fields",
             "fuzzy_weights", "semantic", "blocking", "survivorship"}


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
            known.add(fname)
            steps, default = None, None
            if isinstance(spec, str):
                steps = [spec]
            elif isinstance(spec, list):
                steps = spec
            elif isinstance(spec, dict):
                extra = set(spec) - {"normalize", "default"}
                if extra:
                    err(path, f"unknown key(s) {sorted(extra)}; allowed: normalize, default")
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
                specs[fname] = FieldSpec(tuple(steps), default)

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
    blocking = doc.get("blocking")
    if not isinstance(blocking, list) or not blocking:
        err("blocking", "required: a non-empty list of field groups, e.g. [[\"email\"], [\"last_name\", \"postal_code\"]]. "
                        "Without blocking, no candidates are ever found")
    else:
        if len(blocking) > MAX_BLOCKING_GROUPS:
            err("blocking", f"at most {MAX_BLOCKING_GROUPS} groups")
        for i, group in enumerate(blocking):
            members = field_list(f"blocking[{i}]", group, allow_empty=False)
            if members and members in groups:
                err(f"blocking[{i}]", "duplicate of an earlier group")
            elif members:
                groups.append(members)

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
    )


def config_to_document(config: DomainConfig) -> dict:
    """The inverse of parse_domain_document(): a config as an editable
    document. `export` uses this so a built-in domain can be dumped,
    tweaked, and loaded back as an override."""
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
        doc["fields"][f] = entry
    doc["deterministic_fields"] = list(config.deterministic_fields)
    doc["fuzzy_weights"] = dict(config.fuzzy_weights)
    doc["semantic"] = {"fields": list(config.semantic_fields), "weight": config.semantic_weight}
    doc["blocking"] = [list(g) for g in config.blocking_field_groups]
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
