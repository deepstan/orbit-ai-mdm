"""
Standardization layer: normalizes raw source attributes into a consistent
shape before matching. This is deliberately simple, rule-based
normalization for the MVP — an obvious future upgrade is to swap in
libpostal for address parsing.
"""
from __future__ import annotations

import re
import unicodedata

_WS_RE = re.compile(r"\s+")
_NON_DIGIT_RE = re.compile(r"\D+")

# Common suffixes/prefixes that shouldn't affect name matching
_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = _strip_accents(str(value))
    value = _WS_RE.sub(" ", value.strip())
    return value


def normalize_name(value: str | None) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[.,]", "", value)
    tokens = [t for t in value.split(" ") if t and t not in _NAME_SUFFIXES]
    return " ".join(tokens)


def normalize_email(value: str | None) -> str:
    if not value:
        return ""
    return clean_text(value).lower()


def normalize_phone(value: str | None) -> str:
    """Digits only, and drop a leading country code '1' for 11-digit US numbers
    so '+1 415-555-0100' and '415.555.0100' normalize to the same value."""
    if not value:
        return ""
    digits = _NON_DIGIT_RE.sub("", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def normalize_postal_code(value: str | None) -> str:
    if not value:
        return ""
    value = clean_text(value).upper().replace(" ", "")
    # Collapse ZIP+4 (e.g. "94107-1234") down to the 5-digit ZIP for matching
    if re.fullmatch(r"\d{9}", value):
        return value[:5]
    if re.fullmatch(r"\d{5}-\d{4}", value):
        return value[:5]
    return value


def normalize_address_line(value: str | None) -> str:
    value = clean_text(value).lower()
    replacements = {
        r"\bstreet\b": "st",
        r"\bavenue\b": "ave",
        r"\bboulevard\b": "blvd",
        r"\bdrive\b": "dr",
        r"\broad\b": "rd",
        r"\bapartment\b": "apt",
        r"\bsuite\b": "ste",
        r"\bnumber\b": "no",
    }
    for pattern, repl in replacements.items():
        value = re.sub(pattern, repl, value)
    value = re.sub(r"[.,#]", "", value)
    return _WS_RE.sub(" ", value).strip()


def standardize_customer(raw: dict) -> dict:
    """Takes a raw source payload (arbitrary keys) and returns the
    canonical, normalized customer attribute dict used for matching.
    Missing fields become empty strings, never raise."""
    return {
        "first_name": normalize_name(raw.get("first_name")),
        "last_name": normalize_name(raw.get("last_name")),
        "email": normalize_email(raw.get("email")),
        "phone": normalize_phone(raw.get("phone")),
        "address_line1": normalize_address_line(raw.get("address_line1")),
        "city": normalize_name(raw.get("city")),
        "state": clean_text(raw.get("state")).upper(),
        "postal_code": normalize_postal_code(raw.get("postal_code")),
        "country": clean_text(raw.get("country")).upper() or "US",
    }


def normalize_sku(value: str | None) -> str:
    """SKUs/UPCs are typically case- and whitespace/dash-insensitive
    identifiers -- normalize aggressively since these are meant to be
    exact-match keys, not fuzzy-compared."""
    if not value:
        return ""
    return re.sub(r"[\s\-_]", "", clean_text(value)).upper()


def normalize_product_name(value: str | None) -> str:
    value = clean_text(value).lower()
    # Common catalog abbreviations that shouldn't count as a real difference
    replacements = {
        r"\boz\b": "ounce",
        r"\bpk\b": "pack",
        r"\bct\b": "count",
        r"\bin\b": "inch",
    }
    for pattern, repl in replacements.items():
        value = re.sub(pattern, repl, value)
    return _WS_RE.sub(" ", value).strip()


def standardize_product(raw: dict) -> dict:
    """Canonical, normalized product attribute dict used for matching.
    Missing fields become empty strings, never raise -- same contract as
    standardize_customer()."""
    return {
        "sku": normalize_sku(raw.get("sku")),
        "upc": normalize_sku(raw.get("upc")),  # same normalization rules apply
        "product_name": normalize_product_name(raw.get("product_name")),
        "brand": normalize_name(raw.get("brand")),
        "category": normalize_name(raw.get("category")),
        "description": clean_text(raw.get("description")).lower(),
        "price": clean_text(str(raw.get("price"))) if raw.get("price") is not None else "",
    }


def normalize_tax_id(value: str | None) -> str:
    """Tax IDs / EINs / VAT numbers: strip formatting, keep alphanumerics
    only, uppercase -- these are exact-match identifiers, like SKUs."""
    if not value:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", value).upper()


def standardize_vendor(raw: dict) -> dict:
    """Canonical, normalized vendor/supplier attribute dict used for
    matching. Missing fields become empty strings, never raise."""
    return {
        "company_name": normalize_name(raw.get("company_name")),
        "tax_id": normalize_tax_id(raw.get("tax_id")),
        "duns_number": normalize_sku(raw.get("duns_number")),  # same "strip separators" logic applies
        "contact_email": normalize_email(raw.get("contact_email")),
        "contact_phone": normalize_phone(raw.get("contact_phone")),
        "address_line1": normalize_address_line(raw.get("address_line1")),
        "city": normalize_name(raw.get("city")),
        "state": clean_text(raw.get("state")).upper(),
        "postal_code": normalize_postal_code(raw.get("postal_code")),
        "country": clean_text(raw.get("country")).upper() or "US",
    }


STANDARDIZERS = {
    "customer": standardize_customer,
    "product": standardize_product,
    "vendor": standardize_vendor,
}


def standardize(domain: str, raw: dict) -> dict:
    """Dispatches to the right standardizer for a domain. Raises KeyError
    with a clear message for an unregistered domain rather than silently
    returning something wrong."""
    try:
        return STANDARDIZERS[domain](raw)
    except KeyError:
        raise KeyError(f"No standardizer registered for domain '{domain}'. "
                        f"Known domains: {list(STANDARDIZERS)}") from None
