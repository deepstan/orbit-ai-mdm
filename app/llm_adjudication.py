"""
LLM adjudication (Phase 2 item #2) — architecture doc Section 5.1:

    "LLM adjudication (selective): only for the ambiguous middle band
    where a large model can reason over context ... used sparingly to
    control cost."

This module calls the Anthropic Messages API directly via `urllib`
(stdlib only — no `anthropic` package needed) so it stays consistent with
the rest of this codebase's zero-dependency-for-core-logic approach.

**Not testable in this sandbox** — there's no network access here, so
this code has NOT been run against the real API. It's written to the
documented Messages API contract and fails safe (falls back to the
existing review-queue behavior) on any error, missing API key, or
disabled flag — so turning this on can't make matching *worse*, only
potentially reduce how much lands in the human queue.

Enable it with:
    export ANTHROPIC_API_KEY=sk-ant-...
    export ENABLE_LLM_ADJUDICATION=true

Only records already in the ambiguous band (see ADJUDICATION_SCORE_RANGE
below) are ever sent to the LLM — clear auto-merges and clear non-matches
never incur an API call, to control cost as the architecture doc specifies.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Optional

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
MODEL = "claude-sonnet-4-6"  # keep in sync with whatever your account has access to

# Only the genuinely ambiguous band gets sent to the LLM. Below this,
# fuzzy scoring already handles it (auto-merge or clear no-match); the
# architecture doc calls this out explicitly as a cost-control measure.
ADJUDICATION_SCORE_RANGE = (0.55, 0.85)


@dataclass
class AdjudicationResult:
    is_match: bool
    confidence: float
    explanation: str


def is_enabled() -> bool:
    """Check whether LLM adjudication is switched on.

    Both conditions must hold: ``ENABLE_LLM_ADJUDICATION`` is ``"1"``,
    ``"true"`` or ``"yes"`` (any case), AND ``ANTHROPIC_API_KEY`` is set to a
    non-empty value. Read from the environment on every call, so changes
    take effect without a restart.

    Returns:
        bool: ``True`` if the pipeline should send ambiguous pairs to the LLM.
    """
    return (
        os.environ.get("ENABLE_LLM_ADJUDICATION", "").lower() in ("1", "true", "yes")
        and bool(os.environ.get("ANTHROPIC_API_KEY"))
    )


def in_adjudication_band(score: float) -> bool:
    """Check whether a match score is in the ambiguous range sent to the LLM.

    Args:
        score: Match score from ``matching.score_pair`` (0-1).

    Returns:
        bool: ``True`` if ``0.55 <= score <= 0.85`` (both ends included; see
        ``ADJUDICATION_SCORE_RANGE``). Scores above 0.85 but below the
        auto-merge threshold (0.92) go to human review without an LLM call.
    """
    lo, hi = ADJUDICATION_SCORE_RANGE
    return lo <= score <= hi


_DOMAIN_FRAMING = {
    "customer": "refer to the same real person",
    "product": "refer to the same real-world product (e.g. the same item sold under "
               "slightly different catalog data by different source systems)",
}


def _build_prompt(record_a: dict, record_b: dict, fuzzy_reasons: list[str], domain: str) -> str:
    """Build the prompt text asking the model whether two records match.

    The prompt includes both records as pretty-printed JSON, the fuzzy
    matcher's reasons, a domain-specific description of what "the same"
    means (customer and product have custom wording; any other domain, e.g.
    vendor, gets a generic one), and an instruction to reply with ONLY a
    JSON object: ``{"is_match": ..., "confidence": ..., "explanation": ...}``.

    Args:
        record_a: Standardized attributes of the first record.
        record_b: Standardized attributes of the second record.
        fuzzy_reasons: Reasons from ``score_pair``; "no strong signals" is
            used if empty.
        domain: Domain name, used in the wording.

    Returns:
        str: The full user message to send.
    """
    framing = _DOMAIN_FRAMING.get(domain, f"refer to the same '{domain}' entity")
    return f"""You are adjudicating whether two {domain} records from a master
data management system {framing}.

Record A: {json.dumps(record_a, indent=2)}
Record B: {json.dumps(record_b, indent=2)}

Automated fuzzy matching found: {"; ".join(fuzzy_reasons) if fuzzy_reasons else "no strong signals"}

Consider plausible real-world explanations for why these might be the same
{domain} entity recorded slightly differently by two systems, as well as
reasons they might genuinely be different {domain} entities.

Respond with ONLY a JSON object, no other text, in this exact shape:
{{"is_match": true or false, "confidence": a number 0.0-1.0, "explanation": "one sentence why"}}"""


def adjudicate(record_a: dict, record_b: dict, fuzzy_reasons: list[str], domain: str = "customer") -> Optional[AdjudicationResult]:
    """Calls the Anthropic API to adjudicate an ambiguous match. Returns
    None on any failure (missing key, network error, malformed response)
    so the caller can safely fall back to the human review queue — this
    function is intentionally designed to never raise.

    Sends one request to the Messages API (model ``MODEL``, max 300 output
    tokens, 15-second timeout), joins the text blocks of the reply, and
    parses them as JSON.

    Caveats in the current code:

    - If the model wraps its JSON in a Markdown code fence or adds any
      other text, parsing fails and ``None`` is returned.
    - Only ``URLError``, ``HTTPError``, ``KeyError``, ``ValueError`` and
      ``JSONDecodeError`` are caught. A socket timeout while reading the
      response (``TimeoutError``), or a reply that is valid JSON but not an
      object (``TypeError``), would still raise.

    Args:
        record_a: Standardized attributes of the first record.
        record_b: Standardized attributes of the second record.
        fuzzy_reasons: Reasons from the fuzzy matcher, passed to the model
            as context.
        domain: Domain of the records. Defaults to ``"customer"``.

    Returns:
        AdjudicationResult | None: ``is_match``, ``confidence`` (0-1 as
        reported by the model) and a one-sentence ``explanation``; or
        ``None`` if there is no API key or the call/parse failed.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    payload = {
        "model": MODEL,
        "max_tokens": 300,
        "messages": [{"role": "user", "content": _build_prompt(record_a, record_b, fuzzy_reasons, domain)}],
    }

    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        text = "".join(block.get("text", "") for block in data.get("content", []) if block.get("type") == "text")
        parsed = json.loads(text.strip())
        return AdjudicationResult(
            is_match=bool(parsed["is_match"]),
            confidence=float(parsed["confidence"]),
            explanation=str(parsed.get("explanation", "")),
        )
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError, json.JSONDecodeError) as e:
        # Fail safe: any problem (network, bad JSON from the model, auth
        # error) just means "no adjudication happened" -- never crash the
        # ingestion pipeline over an optional enrichment step.
        print(f"[llm_adjudication] adjudication failed, falling back to review queue: {e}")
        return None
