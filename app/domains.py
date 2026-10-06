"""
Domain registry (Phase 2 item: Product domain / multi-domain proof).

This is the piece that makes the platform genuinely multi-domain rather
than "customer MDM with a Product-shaped afterthought": every
domain-specific choice (schema, which fields are deterministic identity
keys, fuzzy-match weights, which fields feed the semantic-similarity
signal, blocking strategy, survivorship rules) lives in ONE place per
domain, as a `DomainConfig`. `matching.py`, `survivorship.py`, and
`pipeline.py` are all written against this config, not against
hardcoded "first_name"/"last_name" field names -- that's what makes
adding a third domain later a config addition, not an engine rewrite.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import CUSTOMER_SCHEMA_FIELDS, PRODUCT_SCHEMA_FIELDS, VENDOR_SCHEMA_FIELDS
from .normalizers import FieldSpec
from .standardize import STANDARDIZERS, standardize_customer, standardize_product, standardize_vendor
from .survivorship import FieldStrategy


@dataclass
class DomainConfig:
    name: str
    schema_fields: list[str]
    standardizer: callable

    # Fields where an exact (post-standardization) match on EITHER ONE is
    # treated as a deterministic identity match (high confidence, no fuzzy
    # scoring needed). e.g. customer email/phone, product sku/upc.
    deterministic_fields: list[str]

    # Weight of each field in the fuzzy scorer. Must sum to roughly 1.0
    # across (fuzzy_weights + the implicit semantic-name weight) for scores
    # to stay in a sane [0,1] range -- see matching.py's score_pair().
    fuzzy_weights: dict[str, float]

    # Which fields get concatenated into one string for the semantic
    # embedding signal (see embeddings.py). Customer: person name.
    # Product: brand + product name (catches "Nike Air Max 90" vs
    # "Air Max 90 by Nike").
    semantic_fields: list[str]
    semantic_weight: float

    # Groups of fields combined into blocking keys (see matching.py's
    # blocking_keys()) -- each group becomes one candidate-generation key.
    blocking_field_groups: list[list[str]]

    # Per-field survivorship strategy -- same semantics as
    # survivorship.FIELD_STRATEGIES, just scoped per domain now.
    field_strategies: dict[str, FieldStrategy] = field(default_factory=dict)
    default_strategy: FieldStrategy = "most_recent"

    # Declarative description of how each field is standardized (see
    # app/normalizers.py). For a customer-defined domain this IS the
    # standardizer. For the built-ins it describes their Python
    # standardizer, and a test proves the two agree. It feeds the blocking
    # fingerprint in app/indexing.py.
    field_specs: dict = field(default_factory=dict)
    source: str = "builtin"      # "builtin", or the config file it was loaded from
    description: str = ""
    override: bool = False        # only meaningful on a loaded config: replaces a built-in


CUSTOMER_CONFIG = DomainConfig(
    name="customer",
    schema_fields=CUSTOMER_SCHEMA_FIELDS,
    standardizer=standardize_customer,
    deterministic_fields=["email", "phone"],
    fuzzy_weights={
        "first_name": 0.15,
        "last_name": 0.20,
        "address_line1": 0.20,
        "city": 0.10,
        "postal_code": 0.20,
    },
    semantic_fields=["first_name", "last_name"],
    semantic_weight=0.15,
    blocking_field_groups=[
        ["last_name", "postal_code"],
        ["phone"],
        ["email"],
        ["last_name", "first_name"],
    ],
    field_strategies={
        "email": "most_recent",
        "phone": "most_recent",
        "address_line1": "most_recent",
        "city": "most_recent",
        "state": "most_recent",
        "postal_code": "most_recent",
        "country": "most_complete",
        "first_name": "most_complete",
        "last_name": "most_complete",
    },
)

PRODUCT_CONFIG = DomainConfig(
    name="product",
    schema_fields=PRODUCT_SCHEMA_FIELDS,
    standardizer=standardize_product,
    deterministic_fields=["sku", "upc"],
    fuzzy_weights={
        "brand": 0.20,
        "category": 0.15,
        "description": 0.10,
    },
    semantic_fields=["brand", "product_name"],
    semantic_weight=0.35,  # product names vary in wording far more than person names do
    blocking_field_groups=[
        ["sku"],
        ["upc"],
        ["brand", "category"],
    ],
    field_strategies={
        "sku": "most_recent",
        "upc": "most_recent",
        "product_name": "most_complete",
        "brand": "most_complete",
        "category": "most_complete",
        "description": "most_recent",
        "price": "most_recent",
    },
)

VENDOR_CONFIG = DomainConfig(
    name="vendor",
    schema_fields=VENDOR_SCHEMA_FIELDS,
    standardizer=standardize_vendor,
    # A vendor can be uniquely identified by tax ID, DUNS number, or its
    # primary contact email -- any one exact match is a strong signal.
    deterministic_fields=["tax_id", "duns_number", "contact_email"],
    fuzzy_weights={
        "address_line1": 0.20,
        "city": 0.10,
        "postal_code": 0.20,
        "contact_phone": 0.15,
    },
    semantic_fields=["company_name"],
    semantic_weight=0.35,  # company names vary in wording ("Inc" vs "Incorporated", DBA names, etc.)
    blocking_field_groups=[
        ["tax_id"],
        ["duns_number"],
        ["contact_email"],
        ["company_name", "postal_code"],
    ],
    field_strategies={
        "company_name": "most_complete",
        "tax_id": "most_recent",
        "duns_number": "most_recent",
        "contact_email": "most_recent",
        "contact_phone": "most_recent",
        "address_line1": "most_recent",
        "city": "most_recent",
        "state": "most_recent",
        "postal_code": "most_recent",
        "country": "most_complete",
    },
)

def _spec(*steps, default=None) -> FieldSpec:
    return FieldSpec(tuple(steps), default)


# How each built-in standardizer is built from the vocabulary in
# app/normalizers.py. tests/test_domain_config.py proves these reproduce
# standardize_customer/product/vendor exactly, so they can't silently drift.
CUSTOMER_CONFIG.field_specs = {
    "first_name": _spec("name"), "last_name": _spec("name"), "email": _spec("email"),
    "phone": _spec("phone"), "address_line1": _spec("address_line"), "city": _spec("name"),
    "state": _spec("text", "upper"), "postal_code": _spec("postal_code"),
    "country": _spec("text", "upper", default="US"),
}
PRODUCT_CONFIG.field_specs = {
    "sku": _spec("identifier"), "upc": _spec("identifier"), "product_name": _spec("product_name"),
    "brand": _spec("name"), "category": _spec("name"), "description": _spec("lower"), "price": _spec("text"),
}
VENDOR_CONFIG.field_specs = {
    "company_name": _spec("name"), "tax_id": _spec("alnum_id"), "duns_number": _spec("identifier"),
    "contact_email": _spec("email"), "contact_phone": _spec("phone"), "address_line1": _spec("address_line"),
    "city": _spec("name"), "state": _spec("text", "upper"), "postal_code": _spec("postal_code"),
    "country": _spec("text", "upper", default="US"),
}

# The shipped domains. Never mutated; REGISTRY below is the live, editable view.
BUILTIN_DOMAINS: dict[str, DomainConfig] = {
    "customer": CUSTOMER_CONFIG,
    "product": PRODUCT_CONFIG,
    "vendor": VENDOR_CONFIG,
}

# Live registry: the built-ins plus whatever app/domain_config.py loads from
# customer config files (which may also replace a built-in, explicitly).
REGISTRY: dict[str, DomainConfig] = dict(BUILTIN_DOMAINS)


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
