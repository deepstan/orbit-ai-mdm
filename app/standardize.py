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
    """Remove accent marks from letters (e.g. ``"José"`` -> ``"Jose"``).

    Splits each character into base letter + combining marks (Unicode NFKD)
    and drops the marks. NFKD also turns some compatibility characters into
    plain ones (e.g. the ligature ``"ﬁ"`` becomes ``"fi"``).

    Args:
        s: Text to clean.

    Returns:
        str: The text without accents.
    """
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


def clean_text(value: str | None) -> str:
    r"""Basic text cleanup that every other normalizer builds on.

    Converts the value to ``str``, removes accents, trims both ends and
    collapses any run of whitespace (spaces, tabs, newlines) into one space.
    Case is left unchanged.

    Args:
        value: Any value, or ``None``.

    Returns:
        str: The cleaned text, or ``""`` for ``None``/empty input.

    Example:
        ``clean_text("  Zoë\t Smith ")`` -> ``"Zoe Smith"``
    """
    if not value:
        return ""
    value = _strip_accents(str(value))
    value = _WS_RE.sub(" ", value.strip())
    return value


def normalize_name(value: str | None) -> str:
    """Normalize a person, company, city or brand name for matching.

    Steps: ``clean_text``, lowercase, remove periods and commas, then drop
    any word that is a generational suffix (``jr``, ``sr``, ``ii``, ``iii``,
    ``iv``) wherever it appears.

    Args:
        value: The raw name, or ``None``.

    Returns:
        str: The normalized name, or ``""``.

    Example:
        ``normalize_name("Smith, Jr.")`` -> ``"smith"``
    """
    value = clean_text(value).lower()
    value = re.sub(r"[.,]", "", value)
    tokens = [t for t in value.split(" ") if t and t not in _NAME_SUFFIXES]
    return " ".join(tokens)


def normalize_email(value: str | None) -> str:
    """Normalize an email address: clean whitespace/accents and lowercase it.

    No other rewriting is done (dots and ``+tags`` in Gmail addresses are
    kept as they are).

    Args:
        value: The raw email, or ``None``.

    Returns:
        str: The normalized email, or ``""``.
    """
    if not value:
        return ""
    return clean_text(value).lower()


def normalize_phone(value: str | None) -> str:
    """Digits only, and drop a leading country code '1' for 11-digit US numbers
    so '+1 415-555-0100' and '415.555.0100' normalize to the same value.

    Args:
        value: The raw phone number as a string, or ``None``. It must be a
            string; passing an int raises ``TypeError``.

    Returns:
        str: Only the digits, or ``""``. Non-US numbers are left as their
        full digit string.

    Example:
        ``normalize_phone("+1 (415) 555-0100")`` -> ``"4155550100"``
    """
    if not value:
        return ""
    digits = _NON_DIGIT_RE.sub("", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def normalize_postal_code(value: str | None) -> str:
    """Normalize a postal/ZIP code for matching.

    Cleans the text, uppercases it and removes spaces. A US ZIP+4 in either
    form (``"94107-1234"`` or ``"941071234"``) is reduced to the 5-digit ZIP.
    Other formats (e.g. UK ``"SW1A 1AA"`` -> ``"SW1A1AA"``) are kept whole.

    Args:
        value: The raw postal code, or ``None``.

    Returns:
        str: The normalized code, or ``""``.
    """
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
    """Normalize a street address line so spelling variants compare equal.

    Steps: ``clean_text``, lowercase, replace common street words with their
    abbreviation (street->st, avenue->ave, boulevard->blvd, drive->dr,
    road->rd, apartment->apt, suite->ste, number->no), remove ``.``, ``,``
    and ``#``, and collapse spaces.

    Args:
        value: The raw address line, or ``None``.

    Returns:
        str: The normalized address, or ``""``.

    Example:
        ``normalize_address_line("123 Main Street, Apt. #4")`` -> ``"123 main st apt 4"``
    """
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
    Missing fields become empty strings, never raise.

    Field rules: names and city use ``normalize_name``; ``email``,
    ``phone``, ``address_line1`` and ``postal_code`` use their own
    normalizers; ``state`` is cleaned and uppercased; ``country`` is cleaned
    and uppercased and defaults to ``"US"`` when missing. Keys in ``raw``
    that aren't in the customer schema are ignored.

    Args:
        raw: The source payload as a dict.

    Returns:
        dict: The nine canonical customer fields (see
        ``models.CUSTOMER_SCHEMA_FIELDS``).
    """
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
    exact-match keys, not fuzzy-compared.

    Removes spaces, dashes and underscores and uppercases the result. Also
    used for UPCs and DUNS numbers.

    Args:
        value: The raw identifier, or ``None``.

    Returns:
        str: The normalized identifier, or ``""``.

    Example:
        ``normalize_sku("ab-123 x")`` -> ``"AB123X"``
    """
    if not value:
        return ""
    return re.sub(r"[\s\-_]", "", clean_text(value)).upper()


def normalize_product_name(value: str | None) -> str:
    """Normalize a product title for matching.

    Steps: ``clean_text``, lowercase, expand common catalog abbreviations as
    whole words (oz->ounce, pk->pack, ct->count, in->inch), then collapse
    spaces.

    Note: the ``in``->``inch`` rule applies to the word "in" anywhere, so
    ``"made in usa"`` becomes ``"made inch usa"``. Both sides of a
    comparison get the same treatment, so matching still works, but the
    normalized value can look odd.

    Args:
        value: The raw product name, or ``None``.

    Returns:
        str: The normalized product name, or ``""``.
    """
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
    standardize_customer().

    Field rules: ``sku`` and ``upc`` use ``normalize_sku``;
    ``product_name`` uses ``normalize_product_name``; ``brand`` and
    ``category`` use ``normalize_name``; ``description`` is cleaned and
    lowercased; ``price`` is converted to a cleaned string (``""`` when
    missing; ``0`` is kept as ``"0"``).

    Args:
        raw: The source payload as a dict.

    Returns:
        dict: The seven canonical product fields (see
        ``models.PRODUCT_SCHEMA_FIELDS``).
    """
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
    only, uppercase -- these are exact-match identifiers, like SKUs.

    Args:
        value: The raw tax ID as a string, or ``None``.

    Returns:
        str: Uppercase letters and digits only, or ``""``.

    Example:
        ``normalize_tax_id("12-3456789")`` -> ``"123456789"``
    """
    if not value:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", value).upper()


def standardize_vendor(raw: dict) -> dict:
    """Canonical, normalized vendor/supplier attribute dict used for
    matching. Missing fields become empty strings, never raise.

    Field rules: ``company_name`` and ``city`` use ``normalize_name``;
    ``tax_id`` uses ``normalize_tax_id``; ``duns_number`` uses
    ``normalize_sku``; contact email/phone, address and postal code use their
    own normalizers; ``state`` is uppercased; ``country`` is uppercased and
    defaults to ``"US"``.

    Args:
        raw: The source payload as a dict.

    Returns:
        dict: The ten canonical vendor fields (see
        ``models.VENDOR_SCHEMA_FIELDS``).
    """
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
    returning something wrong.

    Args:
        domain: Domain name, e.g. ``"customer"``, ``"product"``, ``"vendor"``.
        raw: The source payload as a dict.

    Returns:
        dict: The canonical, normalized attributes for that domain.

    Raises:
        KeyError: If no standardizer is registered for ``domain``.
    """
    try:
        return STANDARDIZERS[domain](raw)
    except KeyError:
        raise KeyError(f"No standardizer registered for domain '{domain}'. "
                        f"Known domains: {list(STANDARDIZERS)}") from None
