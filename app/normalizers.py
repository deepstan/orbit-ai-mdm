"""
Declarative field standardization.

A customer-defined domain can't ship Python code, and shouldn't: a config
file that can execute code is a security hole and a support nightmare. So
instead of writing a standardizer function, a domain config names a short
list of steps from the FIXED vocabulary below, e.g.

    "state":   {"normalize": ["text", "upper"]}
    "country": {"normalize": ["text", "upper"], "default": "US"}

The steps are the same functions the built-in Customer/Product/Vendor
standardizers are made of (app/standardize.py), so a custom domain gets
exactly the same behavior as a built-in one for the same step. Adding a
new capability means adding a step here (reviewed code), not letting a
config file define one.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from .standardize import (
    clean_text,
    normalize_address_line,
    normalize_email,
    normalize_name,
    normalize_phone,
    normalize_postal_code,
    normalize_product_name,
    normalize_sku,
    normalize_tax_id,
)

# name -> (function, one-line description). Each function takes a str and returns a str.
_PRESETS = {
    "text": (clean_text, "trim, collapse whitespace, strip accents (keeps case)"),
    "lower": (lambda v: clean_text(v).lower(), "text, then lowercase"),
    "upper": (lambda v: clean_text(v).upper(), "text, then uppercase"),
    "name": (normalize_name, "lowercase; drops . and , ; drops suffixes Jr/Sr/II/III/IV"),
    "email": (normalize_email, "trim and lowercase"),
    "phone": (normalize_phone, "digits only; drops a leading US '1' from 11-digit numbers"),
    "postal_code": (normalize_postal_code, "uppercase, no spaces; ZIP+4 collapses to 5 digits"),
    "address_line": (normalize_address_line, "lowercase; street/avenue/boulevard/... abbreviated; punctuation dropped"),
    "identifier": (normalize_sku, "SKU/UPC/DUNS style: drops spaces, dashes, underscores; uppercase"),
    "alnum_id": (normalize_tax_id, "tax-ID style: keeps only letters and digits; uppercase"),
    "product_name": (normalize_product_name, "lowercase; oz/pk/ct/in expanded to ounce/pack/count/inch"),
    "digits": (lambda v: re.sub(r"\D+", "", v), "digits only"),
}
NORMALIZERS = {name: fn for name, (fn, _) in _PRESETS.items()}
NORMALIZER_DOCS = {name: doc for name, (_, doc) in _PRESETS.items()}


@dataclass(frozen=True)
class FieldSpec:
    """How one field is standardized: steps applied in order, then an
    optional literal default if the result is empty."""
    normalize: tuple = ("text",)
    default: Optional[str] = None

    def apply(self, value) -> str:
        text = "" if value is None else str(value)
        for step in self.normalize:
            text = NORMALIZERS[step](text)
        if text == "" and self.default is not None:
            return self.default
        return text

    def to_json(self) -> dict:
        return {"normalize": list(self.normalize), "default": self.default}


class DeclarativeStandardizer:
    """Callable with the same contract as standardize_customer() and
    friends: raw dict in, dict of exactly the schema fields out, never
    raising on missing or odd values."""

    def __init__(self, schema_fields, specs: dict):
        self.schema_fields = list(schema_fields)
        self.specs = dict(specs)

    def __call__(self, raw: dict) -> dict:
        return {f: self.specs[f].apply(raw.get(f)) for f in self.schema_fields}
