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
from .standardize import standardize_customer, standardize_product, standardize_vendor
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

REGISTRY: dict[str, DomainConfig] = {
    "customer": CUSTOMER_CONFIG,
    "product": PRODUCT_CONFIG,
    "vendor": VENDOR_CONFIG,
}


def get_domain_config(domain: str) -> DomainConfig:
    """Look up the configuration for a domain by name.

    The config bundles everything domain-specific: schema fields,
    standardizer, deterministic identity fields, fuzzy weights, semantic
    fields, blocking groups and survivorship strategies.

    Args:
        domain: Domain name: ``"customer"``, ``"product"`` or ``"vendor"``.

    Returns:
        DomainConfig: The registered configuration for that domain.

    Raises:
        KeyError: If the domain isn't in ``REGISTRY``. The message lists the
            known domains.
    """
    try:
        return REGISTRY[domain]
    except KeyError:
        raise KeyError(f"Unknown domain '{domain}'. Known domains: {list(REGISTRY)}") from None
