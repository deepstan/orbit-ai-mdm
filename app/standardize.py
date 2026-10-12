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


# Common US-English nicknames -> the full given name. Deliberately limited to nicknames that map to ONE name:
# Pat (Patrick/Patricia), Chris, Sam, Jamie, Terry, Alex... are ambiguous and left alone, because guessing wrong
# would merge or split people. String similarity alone can't do this job: Bob~Robert scores 0.50 while
# unrelated names routinely score 0.4-0.6, so without a table a nickname looks like a different person.
NICKNAMES = {
    **dict.fromkeys(["bob", "bobby", "rob", "robbie", "robby"], "robert"),
    **dict.fromkeys(["bill", "billy", "will", "willy", "willie"], "william"),
    **dict.fromkeys(["jim", "jimmy"], "james"), **dict.fromkeys(["mike", "mikey"], "michael"),
    **dict.fromkeys(["liz", "lizzie", "beth", "betty", "eliza", "libby"], "elizabeth"),
    **dict.fromkeys(["peggy", "maggie", "meg", "marge"], "margaret"), **dict.fromkeys(["jack", "johnny"], "john"),
    **dict.fromkeys(["dick", "rick", "ricky", "rich", "richie"], "richard"), **dict.fromkeys(["tom", "tommy"], "thomas"),
    **dict.fromkeys(["chuck", "charlie", "chas"], "charles"), **dict.fromkeys(["joe", "joey"], "joseph"),
    **dict.fromkeys(["jen", "jenny", "jenn"], "jennifer"), **dict.fromkeys(["trish", "tricia", "patty"], "patricia"),
    **dict.fromkeys(["kim", "kimmy"], "kimberly"), **dict.fromkeys(["ted", "teddy", "ed", "eddie", "ned"], "edward"),
    **dict.fromkeys(["hank", "harry"], "henry"), **dict.fromkeys(["kate", "katie", "kathy"], "katherine"),
    "cathy": "catherine", **dict.fromkeys(["dave", "davy"], "david"), **dict.fromkeys(["dan", "danny"], "daniel"),
    "matt": "matthew", "tony": "anthony", **dict.fromkeys(["steve", "stevie"], "steven"),
    **dict.fromkeys(["andy", "drew"], "andrew"), "josh": "joshua", "nick": "nicholas",
    **dict.fromkeys(["sue", "susie", "suzy"], "susan"), **dict.fromkeys(["barb", "barbie"], "barbara"),
    **dict.fromkeys(["deb", "debbie"], "deborah"), "becky": "rebecca", "vicky": "victoria", "cindy": "cynthia",
    "sandy": "sandra", "mandy": "amanda", "pam": "pamela", **dict.fromkeys(["ben", "benny"], "benjamin"),
    "greg": "gregory", "jeff": "jeffrey", **dict.fromkeys(["ken", "kenny"], "kenneth"), "larry": "lawrence",
    "pete": "peter", "phil": "philip", **dict.fromkeys(["ron", "ronnie"], "ronald"), "tim": "timothy",
    **dict.fromkeys(["zach", "zack"], "zachary"), "abby": "abigail", "jess": "jessica",
}


def normalize_given_name(value: str | None) -> str:
    """normalize_name(), plus the FIRST token mapped from a nickname to its full name (Bob -> robert).
    Only the first token: in "Mary Beth" the Beth is a middle name, not a nickname for Elizabeth."""
    tokens = normalize_name(value).split(" ")
    if tokens and tokens[0] in NICKNAMES:
        tokens[0] = NICKNAMES[tokens[0]]
    return " ".join(t for t in tokens if t)


def normalize_suffix(value: str | None) -> str:
    """The generational suffix ending a name ("Park Jr." -> "jr"), else "".
    normalize_name() drops suffixes so "Park Jr" and "Park" can still match
    on surname; this keeps the information so a Jr and a Sr at one address
    can be told apart instead of silently merged."""
    tokens = re.sub(r"[.,]", " ", clean_text(value).lower()).split()
    return tokens[-1] if tokens and tokens[-1] in _NAME_SUFFIXES else ""


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
    from . import domains  # noqa: F401 -- importing it installs each registered domain's standardizer
    try:
        return STANDARDIZERS[domain](raw)
    except KeyError:
        raise KeyError(f"No standardizer registered for domain '{domain}'. "
                        f"Known domains: {list(STANDARDIZERS)}") from None
