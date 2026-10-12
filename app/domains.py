"""
Domain registry: every domain-specific decision lives in ONE place per
domain, as a `DomainConfig` -- schema, how each field is standardized, which
fields identify a record exactly, how fuzzy signals are weighted, how
candidates are found (blocking), which disagreements count against a match,
and how conflicting values are resolved. `matching.py`, `survivorship.py` and
`pipeline.py` are written against this config, not against hardcoded field
names -- which is what makes a customer-defined domain (app/domain_config.py)
a config file instead of an engine change.

The three built-in domains are defined here. They use the SAME declarative
standardizer a customer-defined domain does (app/normalizers.py), so there is
one implementation, not two that could drift. The original hand-written
functions remain in app/standardize.py, and a test proves the declarative
versions reproduce them.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import CUSTOMER_SCHEMA_FIELDS, PRODUCT_SCHEMA_FIELDS, VENDOR_SCHEMA_FIELDS
from .normalizers import DeclarativeStandardizer, FieldSpec, KeySpec
from .standardize import STANDARDIZERS
from .survivorship import FieldStrategy


@dataclass
class DomainConfig:
    name: str
    schema_fields: list[str]
    standardizer: callable

    # An exact (post-standardization) match on ANY of these fields means "same
    # record" -- unless contradicted (see mismatch_penalties). e.g. customer
    # email/phone, product sku/upc.
    deterministic_fields: list[str]

    # Weight of each field in the fuzzy scorer. Only relative sizes matter.
    fuzzy_weights: dict[str, float]

    # Fields concatenated for the semantic-similarity signal (see embeddings.py).
    semantic_fields: list[str]
    semantic_weight: float

    # Groups of fields combined into blocking keys (see matching.py's
    # blocking_keys()). Each group is one candidate-lookup key.
    blocking_field_groups: list[list[str]]

    field_strategies: dict[str, FieldStrategy] = field(default_factory=dict)
    default_strategy: FieldStrategy = "most_recent"

    # Declarative description of how each field is standardized (app/normalizers.py).
    # Feeds the blocking fingerprint in app/indexing.py.
    field_specs: dict = field(default_factory=dict)
    source: str = "builtin"      # "builtin", or the config file it was loaded from
    description: str = ""
    override: bool = False        # only meaningful on a loaded config: replaces a built-in

    # Evidence AGAINST a match. A match score that can only ever go up treats "the
    # emails differ" the same as "no email recorded", and lets an exact identifier
    # override clearly different names. field -> penalty subtracted from the score when
    # both values are present and disagree. Identifier fields (deterministic_fields) are
    # compared exactly; others by similarity (below DISAGREE_BELOW), unless listed in
    # mismatch_exact.
    mismatch_penalties: dict = field(default_factory=dict)
    mismatch_exact: list = field(default_factory=list)
    # Identifiers whose difference is a REAL contradiction even when another identifier
    # matched (two different tax IDs). A second email or phone differing is not: people
    # have several, so those penalties only apply when nothing matched exactly.
    exclusive_fields: list = field(default_factory=list)

    # Blocking keys shared by more than this many records stop being used for lookup:
    # a bucket that large (a placeholder, a very common name) finds everyone and no one.
    max_bucket_size: int = 500

    # Per-field key functions, parallel to blocking_field_groups. Empty = every field "auto".
    blocking_keyspecs: list = field(default_factory=list)
    # Parallel to blocking_field_groups: fields a record must LACK for that group's keys to be generated (a conditional group). Empty = always.
    blocking_unless: list = field(default_factory=list)
    # Fields compared as DATES (agree / nearly agree / disagree) rather than by string similarity or exactly.
    mismatch_date: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    def keyspecs(self) -> list:
        if self.blocking_keyspecs:
            return self.blocking_keyspecs
        return [[KeySpec(f) for f in group] for group in self.blocking_field_groups]


def _spec(*steps, default=None, anon=False, sources=(), present=(), type_="text", formats=()) -> FieldSpec:
    return FieldSpec(tuple(steps), default, anonymous_defaults=anon, sources=tuple(sources), present=tuple(present),
                     type=type_, formats=tuple(formats))


CUSTOMER_CONFIG = DomainConfig(
    name="customer",
    schema_fields=CUSTOMER_SCHEMA_FIELDS,
    standardizer=None,  # set below, from field_specs
    deterministic_fields=["email", "phone"],
    fuzzy_weights={"first_name": 0.15, "last_name": 0.20, "address_line1": 0.20, "city": 0.10, "postal_code": 0.20},
    semantic_fields=["first_name", "last_name"],
    semantic_weight=0.15,
    blocking_field_groups=[["last_name", "postal_code"], ["phone"], ["email"], ["last_name", "first_name"]],
    field_strategies={
        "email": "most_recent", "phone": "most_recent", "address_line1": "most_recent", "city": "most_recent",
        "state": "most_recent", "postal_code": "most_recent", "country": "most_complete",
        "first_name": "most_complete", "last_name": "most_complete", "name_suffix": "most_complete",
    },
)
CUSTOMER_CONFIG.field_specs = {
    "first_name": _spec("given_name", anon=True, present=["present_name"]),
    "last_name": _spec("name", anon=True, present=["present_name"]),
    "name_suffix": _spec("suffix", sources=("name_suffix", "last_name"), present=["present_suffix"]),
    "email": _spec("email", anon=True, present=["present_email"]), "phone": _spec("phone", anon=True, present=["present_phone"]),
    "address_line1": _spec("address_line", present=["present_address"]), "city": _spec("name", present=["present_title"]),
    "state": _spec("text", "upper", present=["present_upper"]), "postal_code": _spec("postal_code", present=["present_postal"]),
    "country": _spec("text", "upper", default="US", present=["present_upper"]),
}
CUSTOMER_CONFIG.mismatch_penalties = {"email": 0.15, "phone": 0.15, "first_name": 0.25, "last_name": 0.15, "name_suffix": 0.40}
CUSTOMER_CONFIG.mismatch_exact = ["name_suffix"]
# Blocking, chosen by MEASUREMENT (python3 benchmark.py; see README), not by guesswork. Each addition fixed a way
# a true duplicate was never even compared:
#   phone as overlapping 4-digit windows   a transposed or mistyped digit still shares a window
#   soundex(last_name) + ZIP3 / first name  Smith/Smyth, Johnson/Jonson: spelling variants share a phonetic code
#   first name + ZIP3                       a surname typo of ANY kind (the phonetic key misses the ones that change the code)
# Cost, on the benchmark: ~8 candidates per record instead of ~4.5, ~9% slower, ~40% more review pairs;
# blocking recall on the positive scenarios 78% -> 100%.
CUSTOMER_CONFIG.blocking_keyspecs = [
    [KeySpec("last_name"), KeySpec("postal_code")],
    [KeySpec("phone", "windows", 4)],
    [KeySpec("email")],
    [KeySpec("last_name"), KeySpec("first_name")],
    [KeySpec("last_name", "soundex"), KeySpec("postal_code", "prefix", 3)],
    [KeySpec("last_name", "soundex"), KeySpec("first_name")],
    [KeySpec("first_name"), KeySpec("postal_code")],
]
CUSTOMER_CONFIG.blocking_field_groups = [[spec.field for spec in group] for group in CUSTOMER_CONFIG.blocking_keyspecs]

PRODUCT_CONFIG = DomainConfig(
    name="product",
    schema_fields=PRODUCT_SCHEMA_FIELDS,
    standardizer=None,
    deterministic_fields=["sku", "upc"],
    fuzzy_weights={"brand": 0.20, "category": 0.15, "description": 0.10},
    semantic_fields=["brand", "product_name"],
    semantic_weight=0.35,  # product names vary in wording far more than person names do
    blocking_field_groups=[["sku"], ["upc"], ["brand", "category"]],
    field_strategies={
        "sku": "most_recent", "upc": "most_recent", "product_name": "most_complete", "brand": "most_complete",
        "category": "most_complete", "description": "most_recent", "price": "most_recent",
    },
)
PRODUCT_CONFIG.field_specs = {
    "sku": _spec("identifier", anon=True, present=["present_id"]), "upc": _spec("identifier", anon=True, present=["present_id"]),
    "product_name": _spec("product_name", present=["present_text"]), "brand": _spec("name", anon=True, present=["present_title"]),
    "category": _spec("name", present=["present_title"]), "description": _spec("lower", present=["present_text"]),
    "price": _spec("text", present=["present_text"], type_="numeric"),
}
# Same SKU but a different UPC is a different product (a variant, a pack size); the reverse -- same UPC,
# different SKUs -- is just two catalogs numbering one item.
PRODUCT_CONFIG.mismatch_penalties = {"sku": 0.30, "upc": 0.40, "brand": 0.20}
PRODUCT_CONFIG.exclusive_fields = ["upc"]

VENDOR_CONFIG = DomainConfig(
    name="vendor",
    schema_fields=VENDOR_SCHEMA_FIELDS,
    standardizer=None,
    # Tax ID, DUNS number, or the primary contact email -- any one exact match is a strong signal.
    deterministic_fields=["tax_id", "duns_number", "contact_email"],
    fuzzy_weights={"address_line1": 0.20, "city": 0.10, "postal_code": 0.20, "contact_phone": 0.15},
    semantic_fields=["company_name"],
    semantic_weight=0.35,  # company names vary in wording ("Inc" vs "Incorporated", DBA names, etc.)
    blocking_field_groups=[["tax_id"], ["duns_number"], ["contact_email"], ["company_name", "postal_code"]],
    field_strategies={
        "company_name": "most_complete", "tax_id": "most_recent", "duns_number": "most_recent",
        "contact_email": "most_recent", "contact_phone": "most_recent", "address_line1": "most_recent",
        "city": "most_recent", "state": "most_recent", "postal_code": "most_recent", "country": "most_complete",
    },
)
VENDOR_CONFIG.field_specs = {
    "company_name": _spec("name", anon=True, present=["present_text"]),  # kept as typed: LLC / IBM / ACME can't be re-cased safely
    "tax_id": _spec("alnum_id", anon=True, present=["present_id"]), "duns_number": _spec("identifier", anon=True, present=["present_id"]),
    "contact_email": _spec("email", anon=True, present=["present_email"]), "contact_phone": _spec("phone", anon=True, present=["present_phone"]),
    "address_line1": _spec("address_line", present=["present_address"]), "city": _spec("name", present=["present_title"]),
    "state": _spec("text", "upper", present=["present_upper"]), "postal_code": _spec("postal_code", present=["present_postal"]),
    "country": _spec("text", "upper", default="US", present=["present_upper"]),
}
# Two different tax IDs or DUNS numbers are two legal entities, whatever their names and addresses say.
VENDOR_CONFIG.mismatch_penalties = {"tax_id": 0.40, "duns_number": 0.40, "contact_email": 0.10, "contact_phone": 0.10}
VENDOR_CONFIG.exclusive_fields = ["tax_id", "duns_number"]

for _cfg in (CUSTOMER_CONFIG, PRODUCT_CONFIG, VENDOR_CONFIG):
    _cfg.standardizer = DeclarativeStandardizer(_cfg.schema_fields, _cfg.field_specs)

# The shipped domains. Never mutated; REGISTRY below is the live, editable view.
BUILTIN_DOMAINS: dict[str, DomainConfig] = {
    "customer": CUSTOMER_CONFIG,
    "product": PRODUCT_CONFIG,
    "vendor": VENDOR_CONFIG,
}

# Live registry: the built-ins plus whatever app/domain_config.py loads from customer config
# files (which may also replace a built-in, explicitly).
REGISTRY: dict[str, DomainConfig] = dict(BUILTIN_DOMAINS)
for _name, _cfg in BUILTIN_DOMAINS.items():
    STANDARDIZERS[_name] = _cfg.standardizer


def register_domain(config: DomainConfig, replace: bool = False) -> None:
    """Adds a domain to the live registry and to the standardizer dispatch
    that pipeline.py and active_learning.py use."""
    if config.name in REGISTRY and not replace:
        raise KeyError(f"Domain '{config.name}' is already defined")
    REGISTRY[config.name] = config
    STANDARDIZERS[config.name] = config.standardizer


def reset_registry() -> None:
    """Back to the built-in domains only (tests; domain_config.reset())."""
    for name in [n for n in REGISTRY if n not in BUILTIN_DOMAINS]:
        del REGISTRY[name]
        STANDARDIZERS.pop(name, None)
    for name, cfg in BUILTIN_DOMAINS.items():
        REGISTRY[name] = cfg
        STANDARDIZERS[name] = cfg.standardizer


def get_domain_config(domain: str) -> DomainConfig:
    try:
        return REGISTRY[domain]
    except KeyError:
        raise KeyError(f"Unknown domain '{domain}'. Known domains: {list(REGISTRY)}") from None
