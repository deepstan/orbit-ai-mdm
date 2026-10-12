"""
Declarative field standardization and blocking-key functions.

A customer-defined domain can't ship Python code, and shouldn't: a config
file that can execute code is a security hole and a support nightmare. So
instead of writing a standardizer function, a domain config names a short
list of steps from the FIXED vocabulary below, e.g.

    "state":   {"normalize": ["text", "upper"]}
    "country": {"normalize": ["text", "upper"], "default": "US"}
    "email":   {"normalize": "email", "anonymous_defaults": true}

The steps are the same functions the built-in domains are made of
(app/standardize.py). Adding a capability means adding a step here
(reviewed code), not letting a config file define one.

Three ideas beyond plain normalization live here:

  * ANONYMOUS VALUES. A placeholder ("none@none.com", "000-000-0000",
    "unknown") is not evidence of anything: two strangers can share one. A
    field can declare placeholders, after which they standardize to
    "missing" -- so they never produce a deterministic match, never a
    blocking key, and never count as agreement.
  * SOURCES. A field can be read from other raw fields (e.g. the generational
    suffix is read from `name_suffix`, else from the end of `last_name`).
  * BLOCKING KEY FUNCTIONS (KeySpec): how a field turns into candidate-lookup
    keys -- whole value, prefix, Soundex (phonetic), or overlapping windows
    (tolerant of a typo or transposition in the middle of an identifier).
"""
from __future__ import annotations

import datetime as _dt
import re
from dataclasses import dataclass
from itertools import product
from typing import Optional

from .protect import mask_value, near_token as protect_near_token, token as protect_token
from .standardize import (
    clean_text,
    normalize_address_line,
    normalize_email,
    normalize_given_name,
    normalize_name,
    normalize_phone,
    normalize_postal_code,
    normalize_product_name,
    normalize_sku,
    normalize_suffix,
    normalize_tax_id,
)

# ---------------------------------------------------------------------------------- SSN and date of birth
# Two fields where a PLACEHOLDER is worse than a missing value: an SSN and a DOB are strong evidence, so a defaulted one
# makes strangers "agree" (a benchmark measured 75% wrong merges for two people with the same common name, the same town and
# a defaulted DOB). These rules are conservative on purpose (a false positive silently discards real data) and every
# rejection is reported with a reason, never with the value. Whatever they cannot know is found by `domains_cli.py profile`.
_SSN_PUBLISHED = {"078051120", "219099999", "457555462", "123456789", "987654321"}  # the Woolworth card, an SSA pamphlet, a famous ad, the obvious ones
_ITIN_GROUPS = set(range(50, 66)) | set(range(70, 89)) | set(range(90, 93)) | set(range(94, 100))
_DOB_DEFAULTS = {"1900-01-01", "1899-12-30", "1899-12-31", "1970-01-01", "1800-01-01", "1753-01-01", "0001-01-01", "1111-11-11", "9999-12-31"}


def check_ssn(raw, itin_ok: bool = False) -> tuple:
    """(nine digits, None) when plausible, else ("", reason). Nothing there at all is not a rejection: ("", None)."""
    text = "" if raw is None else str(raw)
    digits = re.sub(r"\D", "", text)
    if not digits:
        return "", ("no digits" if text.strip() else None)
    if len(digits) != 9:
        return "", "not 9 digits"
    if len(set(digits)) == 1:
        return "", "all the same digit"
    if digits in _SSN_PUBLISHED:
        return "", "a widely published example number"
    area, group, serial = int(digits[:3]), int(digits[3:5]), int(digits[5:])
    if area == 0:
        return "", "area 000"
    if area == 666:
        return "", "area 666"
    if area >= 900 and not (itin_ok and group in _ITIN_GROUPS):
        return "", "area 9xx (not a valid SSN)"
    if group == 0:
        return "", "group 00"
    if serial == 0:
        return "", "serial 0000"
    return digits, None


def _today() -> _dt.date:
    return _dt.date.today()


def check_dob(raw, parse=(), *, max_age_years: int = 120, min_year=None, defaults=(), today=None) -> tuple:
    """(ISO date, None) for a plausible full date; (YYYY, note) for a year alone (kept for matching, not a golden date);
    ("", reason) for a rejection. Dates are never guessed: ISO is always read, anything else only in the `parse` formats the
    domain config declares (so 03/07/1985 is March 7th or July 3rd because the config says so)."""
    text = "" if raw is None else str(raw).strip()
    if not text:
        return "", None
    today = today or _today()
    floor = min_year if min_year is not None else today.year - max_age_years
    if re.fullmatch(r"\d{4}", text):
        year = int(text)
        if floor <= year <= today.year:
            return text, "year only: kept for matching, not stored in the golden record"
        return "", "implausible year"
    parsed = None
    for fmt in ("%Y-%m-%d", *parse):
        try:
            parsed = _dt.datetime.strptime(text, fmt).date()
            break
        except ValueError:
            continue
    if parsed is None:
        return "", "not a real date in a declared format"
    iso = parsed.isoformat()
    if iso in _DOB_DEFAULTS or iso in defaults:
        return "", "a default or system date"
    if parsed > today:
        return "", "in the future"
    if parsed.year < floor:
        return "", "implausible age"
    return iso, None


def _dob_args(spec) -> dict:
    return {"max_age_years": spec.opt("max_age_years", 120), "min_year": spec.opt("min_year", None), "defaults": tuple(spec.opt("defaults", ()))}


# Every vocabulary step stays callable with just a string (it then uses its defaults); the pipeline passes the field's own
# settings (parse formats, options) as the second argument.
def _step_ssn(text, spec=None):
    return check_ssn(text, spec is not None and spec.opt("itin", "reject") == "allow")[0]


def _step_dob(text, spec=None):
    return check_dob(text, spec.parse if spec else (), **(_dob_args(spec) if spec else {}))[0]


def _present_ssn(text, spec=None):
    d = re.sub(r"\D", "", text or "")
    return f"{d[:3]}-{d[3:5]}-{d[5:]}" if len(d) == 9 else ""


def _present_date(text, spec=None):
    v = _step_dob(text, spec)
    return v if len(v) == 10 else ""  # only a COMPLETE date reaches the golden record


# steps that need the field's own settings (parse formats, options) get the FieldSpec as a second argument
CONTEXTUAL = {"ssn", "dob"}
CONTEXTUAL_PRESENT = {"present_ssn", "present_date"}
DIAGNOSERS = {"ssn": lambda text, spec: check_ssn(text, spec.opt("itin", "reject") == "allow")[1],
              "dob": lambda text, spec: check_dob(text, spec.parse, **_dob_args(spec))[1]}
_OPTION_KEYS = {"ssn": {"itin", "near"}, "dob": {"max_age_years", "min_year", "defaults"}}


def freeze_options(options: dict) -> tuple:
    return tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in (options or {}).items()))


def validate_field_options(steps, parse, options) -> list:
    """Problems with a field's `parse` and `options`, as messages; empty when fine."""
    errors, steps = [], list(steps)
    if parse and "dob" not in steps:
        errors.append("'parse' only applies to a field with the 'dob' normalizer")
    allowed = set().union(*(_OPTION_KEYS[s] for s in steps if s in _OPTION_KEYS)) if any(s in _OPTION_KEYS for s in steps) else set()
    for key, value in (options or {}).items():
        if key not in allowed:
            errors.append(f"unknown option '{key}'" + (f"; allowed here: {', '.join(sorted(allowed))}" if allowed else "; this field's normalizers take no options"))
        elif key == "itin" and value not in ("reject", "allow"):
            errors.append("itin must be \"reject\" (default) or \"allow\"")
        elif key == "near" and not isinstance(value, bool):
            errors.append("near must be true or false")
        elif key in ("max_age_years", "min_year") and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
            errors.append(f"{key} must be a whole number of at least 1")
        elif key == "defaults":
            if not isinstance(value, list) or not all(isinstance(x, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", x) for x in value):
                errors.append("defaults must be a list of ISO dates such as \"1950-01-01\"")
    return errors


# name -> (function, one-line description). Each function takes a str and returns a str.
_PRESETS = {
    "text": (clean_text, "trim, collapse whitespace, strip accents (keeps case)"),
    "lower": (lambda v: clean_text(v).lower(), "text, then lowercase"),
    "upper": (lambda v: clean_text(v).upper(), "text, then uppercase"),
    "name": (normalize_name, "lowercase; drops . and , ; drops suffixes Jr/Sr/II/III/IV"),
    "given_name": (normalize_given_name, "like name, and a nickname becomes the full name (Bob -> robert); first word only"),
    "suffix": (normalize_suffix, "the generational suffix (jr, sr, ii, iii, iv) ending the value, else empty"),
    "email": (normalize_email, "trim and lowercase"),
    "phone": (normalize_phone, "digits only; drops a leading US '1' from 11-digit numbers"),
    "postal_code": (normalize_postal_code, "uppercase, no spaces; ZIP+4 collapses to 5 digits"),
    "address_line": (normalize_address_line, "lowercase; street/avenue/boulevard/... abbreviated; punctuation dropped"),
    "identifier": (normalize_sku, "SKU/UPC/DUNS style: drops spaces, dashes, underscores; uppercase"),
    "alnum_id": (normalize_tax_id, "tax-ID style: keeps only letters and digits; uppercase"),
    "product_name": (normalize_product_name, "lowercase; oz/pk/ct/in expanded to ounce/pack/count/inch"),
    "digits": (lambda v: re.sub(r"\D+", "", v), "digits only"),
    "ssn": (_step_ssn, "US SSN: nine digits, or empty when it is implausible (area 000/666/9xx, group 00, serial 0000, all one digit, published examples); option itin: allow keeps ITINs"),
    "dob": (_step_dob, "date of birth as YYYY-MM-DD (or a bare year); ISO plus only the formats in `parse`; empty for impossible, default (1900-01-01, 1970-01-01, ...), future or implausible dates"),
}
NORMALIZERS = {name: fn for name, (fn, _) in _PRESETS.items()}
NORMALIZER_DOCS = {name: doc for name, (_, doc) in _PRESETS.items()}


# ------------------------------------------------------------------------ presentation forms
# The CLEANSED form of a value: what a golden record shows to reporting and downstream systems. Different from the
# matching form on purpose: matching wants "robert" and "5125550123" (lowercase, digits, accents stripped, so they
# compare well); a report wants "Robert" and "(512) 555-0123". Presenters work from the RAW value, so accents
# (José) and deliberate casing (McDonald, DeShawn) survive. Only all-upper or all-lower input is re-cased.
_WS = re.compile(r"\s+")


def _ws(v) -> str:
    return _WS.sub(" ", str(v or "").strip())


def _cap(token: str) -> str:
    t = token.lower()
    if "'" in t:
        parts = t.split("'")
        return "'".join(p.capitalize() for p in parts) if len(parts[0]) == 1 else t.capitalize()  # O'Neil, D'Angelo; Don't
    if t.startswith("mc") and len(t) > 3:
        return "Mc" + t[2:].capitalize()
    return t.capitalize()


def smart_title(value) -> str:
    v = _ws(value)
    if not v or (any(c.islower() for c in v) and any(c.isupper() for c in v)):
        return v  # mixed case was typed deliberately: leave it
    return "".join(p if re.fullmatch(r"[\s\-/]+", p) else _cap(p) for p in re.split(r"([\s\-/]+)", v))


_SUFFIXES = {"jr": "Jr", "sr": "Sr", "ii": "II", "iii": "III", "iv": "IV"}


def present_name(value) -> str:
    tokens = _ws(value).split(" ")
    while tokens and tokens[-1].lower().strip(".,") in _SUFFIXES:  # the suffix is its own field
        tokens.pop()
    return smart_title(" ".join(tokens).rstrip(","))


def present_address(value) -> str:
    t = smart_title(value)
    t = re.sub(r"\b(\d+)(St|Nd|Rd|Th)\b", lambda m: m.group(1) + m.group(2).lower(), t)
    return re.sub(r"\b(Ne|Nw|Se|Sw|Po)\b", lambda m: m.group(1).upper(), t)


def present_phone(value) -> str:
    d = re.sub(r"\D", "", str(value or ""))
    d = d[1:] if len(d) == 11 and d.startswith("1") else d
    return f"({d[:3]}) {d[3:6]}-{d[6:]}" if len(d) == 10 else _ws(value)


def present_postal(value) -> str:
    v = _ws(value).upper()
    d = re.sub(r"\D", "", v)
    return f"{d[:5]}-{d[5:]}" if len(d) == 9 and d == re.sub(r"[\s\-]", "", v) else v


_PRESETS_PRESENT = {
    "present_text": (_ws, "trim and collapse whitespace; nothing else (accents and casing kept)"),
    "present_title": (smart_title, "Title Case, but ONLY if the input was all-upper or all-lower (McDonald stays McDonald)"),
    "present_name": (present_name, "person name: drops a trailing Jr/Sr/II/III/IV (its own field), then title case if needed"),
    "present_suffix": (lambda v: _SUFFIXES.get(normalize_suffix(v), ""), "Jr, Sr, II, III, IV"),
    "present_address": (present_address, "street line: title case if needed; 1st/2nd, NE/SW, PO fixed"),
    "present_email": (lambda v: _ws(v).lower(), "trimmed, lowercase"),
    "present_phone": (present_phone, "US numbers as (512) 555-0123; anything else left as typed"),
    "present_postal": (present_postal, "uppercase; 9-digit ZIP as 12345-6789"),
    "present_upper": (lambda v: _ws(v).upper(), "uppercase (states, countries)"),
    "present_id": (lambda v: _ws(v).upper(), "identifier: trimmed, uppercase"),
    "present_ssn": (_present_ssn, "SSN as 123-45-6789"),
    "present_date": (_present_date, "a complete date as YYYY-MM-DD (a bare year is not a golden date)"),
}
PRESENTERS = {name: fn for name, (fn, _) in _PRESETS_PRESENT.items()}


def run_step(step: str, text: str, spec) -> str:
    fn = NORMALIZERS[step]
    return fn(text, spec) if step in CONTEXTUAL else fn(text)


def run_presenter(step: str, text: str, spec) -> str:
    fn = PRESENTERS[step]
    return fn(text, spec) if step in CONTEXTUAL_PRESENT else fn(text)
PRESENTER_DOCS = {name: doc for name, (_, doc) in _PRESETS_PRESENT.items()}


# --------------------------------------------------------------------------- placeholders
_WORD_PLACEHOLDERS = {"unknown", "unk", "n/a", "na", "none", "null", "nil", "test", "tbd", "anonymous", "anon",
                      "notprovided", "not provided", "noname", "no name", "?", "-", "--"}
_EMAIL_LOCALS = {"none", "noemail", "nomail", "noreply", "donotreply", "unknown", "na", "null", "nobody",
                 "notprovided", "no", "test"}
_EMAIL_DOMAINS = {"none.com", "noemail.com", "nomail.com", "na.com", "nowhere.com", "notprovided.com"}
_PHONE_FAKES = {"1234567890", "0123456789", "9876543210", "5551234567", "5555551212"}
_ID_WORDS = {"NA", "N/A", "NONE", "UNKNOWN", "UNK", "TBD", "NULL", "TEST"}

ANONYMOUS_KINDS = ("email", "phone", "name", "given_name", "identifier", "alnum_id")


def is_placeholder(kind: str, value: str) -> bool:
    """Is this already-standardized value a known stand-in for 'no real value'?
    Deliberately conservative: a false positive silently discards real data,
    so only clear placeholders are listed (all-same-character strings, obvious
    sequences, the usual 'none'/'unknown'/'test' words). Anything else is
    handled by `anonymous` in the field's own config."""
    if not value:
        return False
    if kind == "email":
        local, _, domain = value.partition("@")
        return value in _WORD_PLACEHOLDERS or local in _EMAIL_LOCALS or domain in _EMAIL_DOMAINS
    if kind == "phone":
        return len(value) < 7 or len(set(value)) == 1 or value in _PHONE_FAKES
    if kind in ("name", "given_name"):
        return value in _WORD_PLACEHOLDERS
    if kind == "identifier":
        return value in _ID_WORDS or bool(re.fullmatch(r"(.)\1{3,}", value))
    if kind == "alnum_id":
        return value in _ID_WORDS or bool(re.fullmatch(r"(.)\1{4,}", value))
    return False


# ------------------------------------------------------------------------------ fields
@dataclass(frozen=True)
class FieldSpec:
    """How one field is standardized: steps applied in order; placeholders
    (explicit `anonymous` values, plus the built-in ones if
    `anonymous_defaults`) become missing; then an optional literal default
    if the result is empty. `sources` names the raw fields to read, in
    order, instead of this field's own name."""
    normalize: tuple = ("text",)
    default: Optional[str] = None
    anonymous: tuple = ()
    anonymous_defaults: bool = False
    sources: tuple = ()
    present: tuple = ()  # presentation steps for the CLEANSED form (default: present_text)
    type: str = "text"   # the golden column's type (app/fieldtypes.py); staging stays text
    formats: tuple = ()  # strptime patterns for date/timestamp fields
    parse: tuple = ()    # strptime patterns the `dob` normalizer may READ (ISO is always read); separate from `formats`, which types the golden column
    options: tuple = ()  # (name, value) pairs for the field's normalizers, e.g. (("itin", "allow"),)
    protect: str = ""    # "hash": the MATCHING form is a keyed hash and the CLEANSED form a mask; the plain value lives only in raw (app/protect.py)

    def plain(self, value) -> str:
        """The normalized value BEFORE protection (a read-only tool such as the profiler needs the real value; nothing stores it)."""
        return self._core("" if value is None else str(value))

    def near_tokens(self, value) -> str:
        """For a PROTECTED identifier with option near: keyed hashes of the value itself, of every one-digit-blanked variant and of every
        adjacent-swapped variant, comma-joined. Two values a single typo apart then share a token, without either value being stored.
        Scoring only (never a blocking key). "" when the option is off."""
        if not (self.protect == "hash" and self.opt("near")):
            return ""
        digits = self._core("" if value is None else str(value))
        if not digits:
            return ""
        variants = {digits} | {digits[:i] + "?" + digits[i + 1:] for i in range(len(digits))}
        variants |= {digits[:i] + digits[i + 1] + digits[i] + digits[i + 2:] for i in range(len(digits) - 1) if digits[i] != digits[i + 1]}
        return ",".join(sorted(protect_near_token(v, self.normalize[-1]) for v in variants))

    def _guard(self, text: str) -> str:
        return protect_token(text, self.normalize[-1]) if self.protect == "hash" and text else text

    def opt(self, name: str, default=None):
        return dict(self.options).get(name, default)

    @property
    def reports_rejections(self) -> bool:
        """Does this field explain what its rules discarded? (ssn, dob)"""
        return any(step in DIAGNOSERS for step in self.normalize)

    def diagnose(self, value):
        """Why a RAW value was discarded or only partly kept, as a short reason that never contains the value; None if nothing to report."""
        text = "" if value is None else str(value)
        if not text.strip():
            return None
        for step in self.normalize:
            if step in DIAGNOSERS:
                note = DIAGNOSERS[step](text, self)
                if note:
                    return note
            text = run_step(step, text, self)
        if text and text in self.anonymous:
            return "listed as a placeholder in the domain config"
        if text and self.anonymous_defaults and is_placeholder(self.normalize[-1], text):
            return "a known placeholder value"
        return None

    def _core(self, raw: str) -> str:
        text = raw
        for step in self.normalize:
            text = run_step(step, text, self)
        if text and (text in self.anonymous
                     or (self.anonymous_defaults and is_placeholder(self.normalize[-1], text))):
            return ""
        return text

    def apply(self, value) -> str:
        """The MATCHING form."""
        text = self._core("" if value is None else str(value))
        if text == "" and self.default is not None:
            return self.default
        return self._guard(text)

    def apply_both(self, value) -> tuple:
        """(matching form, cleansed form). A placeholder or empty value is empty in BOTH: a placeholder must never
        reach a golden record. The cleansed form is derived from the RAW value, not from the matching form."""
        raw = "" if value is None else str(value)
        text = self._core(raw)
        if text == "":
            return (self.default, self.default) if self.default is not None else ("", "")
        clean = raw
        for step in (self.present or ("present_text",)):
            clean = run_presenter(step, clean, self)
        if self.protect == "hash":
            clean = mask_value(clean, self.normalize[-1])  # golden records, lineage and screens only ever see the mask
        return self._guard(text), clean

    def to_json(self) -> dict:
        d = {"normalize": list(self.normalize), "default": self.default}
        if self.anonymous:
            d["anonymous"] = sorted(self.anonymous)
        if self.anonymous_defaults:
            d["anonymous_defaults"] = True
        if self.sources:
            d["sources"] = list(self.sources)
        if self.present:
            d["present"] = list(self.present)
        if self.type != "text":
            d["type"] = self.type
        if self.formats:
            d["format"] = list(self.formats)
        if self.parse:
            d["parse"] = list(self.parse)
        if self.options:
            d["options"] = {k: (list(v) if isinstance(v, tuple) else v) for k, v in self.options}
        if self.protect:
            d["protect"] = self.protect
        return d


class DeclarativeStandardizer:
    """Callable with the same contract as the legacy standardize_customer()
    and friends: raw dict in, dict of exactly the schema fields out, never
    raising on missing or odd values."""

    def __init__(self, schema_fields, specs: dict):
        self.schema_fields = list(schema_fields)
        self.specs = dict(specs)

    def standardize_both(self, raw: dict) -> tuple:
        """(matching form, cleansed form) of every schema field. The staging layer stores both."""
        match, clean = {}, {}
        for f in self.schema_fields:
            spec = self.specs[f]
            value = next((raw.get(n) for n in (spec.sources or (f,)) if raw.get(n) not in (None, "")), None)
            match[f], clean[f] = spec.apply_both(value)
            near = spec.near_tokens(value) if spec.opt("near") else ""
            if near:
                match[f + "~near"] = near  # scoring only: see FieldSpec.near_tokens (nothing is stored for a record without the value)
        return match, clean

    def __call__(self, raw: dict) -> dict:
        return self.standardize_both(raw)[0]


# ------------------------------------------------------------------- blocking key functions
def soundex(value: str) -> str:
    """Standard American Soundex: first letter + three digits (Robert -> R163,
    Smith and Smyth -> S530). Phonetically similar names share a code, which
    tolerates the typos and spelling variants prefix keys can't."""
    s = re.sub(r"[^a-z]", "", clean_text(value).lower())
    if not s:
        return ""
    codes = {**dict.fromkeys("bfpv", "1"), **dict.fromkeys("cgjkqsxz", "2"), **dict.fromkeys("dt", "3"),
             "l": "4", **dict.fromkeys("mn", "5"), "r": "6"}
    out, prev = [s[0].upper()], codes.get(s[0], "")
    for ch in s[1:]:
        code = codes.get(ch, "")
        if code and code != prev:
            out.append(code)
        if ch not in "hw":  # h and w don't separate two letters of the same code; vowels do
            prev = code
    return ("".join(out) + "000")[:4]


KEY_KINDS = ("auto", "exact", "prefix", "soundex", "windows", "transpose")
MAX_KEYS_PER_GROUP = 25


@dataclass(frozen=True)
class KeySpec:
    """How ONE field contributes to a blocking key.
      auto     (default) the first 3 characters when the group has several
               fields, the whole value when it stands alone
      exact    the whole value
      prefix   the first `size` characters (default 3)
      soundex  the phonetic code
      windows  every overlapping run of `size` characters (default 4): a
               typo or transposition mid-value still leaves other windows intact
      transpose the whole value AND every variant with two adjacent characters swapped (at most len-1 extra keys). Catches a
               swapped pair of digits like windows do, but each key is nearly unique, so its bucket stays tiny however large the data
    """
    field: str
    kind: str = "auto"
    size: Optional[int] = None

    def keys(self, value: str, multi: bool, auto_prefix: int = 3) -> list:
        if not value:
            return []
        if self.kind == "auto":
            return [value[:auto_prefix]] if multi else [value]
        if self.kind == "exact":
            return [value]
        if self.kind == "prefix":
            return [value[: (self.size or auto_prefix)]]
        if self.kind == "soundex":
            code = soundex(value)
            return [code] if code else []
        if self.kind == "windows":
            n = self.size or 4
            return list(dict.fromkeys(value[i:i + n] for i in range(len(value) - n + 1))) or [value]
        if self.kind == "transpose":
            swaps = [value[:i] + value[i + 1] + value[i] + value[i + 2:] for i in range(len(value) - 1) if value[i] != value[i + 1]]
            return [value] + list(dict.fromkeys(swaps))
        raise ValueError(self.kind)

    def to_json(self):
        if self.kind == "auto" and self.size is None:
            return self.field
        d = {"field": self.field, "as": self.kind}
        if self.size is not None:
            d["size"] = self.size
        return d


def group_keys(specs: list, values: list, auto_prefix: int = 3) -> list:
    """All key bodies for one blocking group: the cartesian product of each
    field's keys (so a windowed phone number yields several keys)."""
    parts = [s.keys(v, len(specs) > 1, auto_prefix) for s, v in zip(specs, values)]
    if not all(parts):
        return []
    return [":".join(combo) for combo in product(*parts)][:MAX_KEYS_PER_GROUP]
