"""
Usage metering + billing hooks (Phase 3 item #3).

What's real here: usage EVENTS are genuinely recorded (every ingest,
every LLM adjudication call, every retrain) and genuinely queryable per
tenant -- that's the metering half, and it's fully testable without any
external service.

What's a stub: actually CHARGING a tenant's card requires a real payment
processor (Stripe, in the architecture doc's framing) with live API
credentials and network access, neither available in this sandbox. So
`BillingProvider` is a pluggable interface with one working
implementation (`LoggingBillingProvider`, which just records what WOULD
be billed) and a documented extension point for a real
`StripeBillingProvider`.

This mirrors how `llm_adjudication.py` and `embeddings.py` handle the
same "real integration needs external access" problem: build the
interface and a safe default, document exactly where the real thing
plugs in, don't fake having built something that wasn't.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from .db import Store
from .models import UsageEvent, now_iso

# Per-unit prices are illustrative only -- a real deployment would pull
# these from a pricing table per tenant's plan, not a hardcoded dict.
DEFAULT_UNIT_PRICES_USD = {
    "record_ingested": 0.002,
    "llm_adjudication_call": 0.01,
    "api_call": 0.0001,
}


def record_usage(store: Store, tenant_id: str, event_type: str, domain: str | None = None,
                  quantity: int = 1, metadata: dict | None = None) -> UsageEvent:
    """Record one billable usage event for a tenant.

    Note: the only callers today are ``api.ingest`` and ``seed_demo.py``,
    both with ``"record_ingested"``. Despite what earlier comments said,
    ``pipeline.py`` and the other API routes do not call this, so
    ``api_call`` and ``llm_adjudication_call`` are never recorded yet.

    Args:
        store: Storage backend.
        tenant_id: Tenant to bill.
        event_type: Kind of usage, e.g. ``"record_ingested"``. Should match
            a key in the price table, or it will be priced at $0.
        domain: Optional domain the usage belongs to.
        quantity: How many units this event represents. Defaults to 1.
        metadata: Optional extra JSON-serializable data. Defaults to ``{}``.

    Returns:
        UsageEvent: The stored event.
    """
    ev = UsageEvent(tenant_id=tenant_id, event_type=event_type, domain=domain,
                     quantity=quantity, metadata=metadata or {})
    store.record_usage_event(ev)
    return ev


@dataclass
class UsageSummary:
    tenant_id: str
    since: str | None
    counts_by_event_type: dict[str, int] = field(default_factory=dict)
    estimated_cost_usd: float = 0.0


def get_usage_summary(store: Store, tenant_id: str, since: str | None = None,
                       unit_prices: dict[str, float] | None = None) -> UsageSummary:
    """Aggregates usage events for a tenant into counts per event type
    plus an estimated cost, using `unit_prices` (or the illustrative
    defaults above). This is what a billing dashboard or a periodic
    invoicing job would call.

    Event types without a price are counted but add $0. If ``unit_prices``
    is given it replaces the defaults entirely (it isn't merged with them).
    Costs are added up as floats and rounded to 4 decimal places at the end.

    Args:
        store: Storage backend.
        tenant_id: Tenant to summarize.
        since: Optional ISO timestamp; only events at or after it count.
        unit_prices: Optional ``{event_type: price_in_usd}`` table. Defaults
            to ``DEFAULT_UNIT_PRICES_USD``.

    Returns:
        UsageSummary: Total quantity per event type and estimated cost in USD.
    """
    prices = unit_prices or DEFAULT_UNIT_PRICES_USD
    events = store.list_usage_events(tenant_id, since=since)

    counts: dict[str, int] = {}
    cost = 0.0
    for ev in events:
        counts[ev.event_type] = counts.get(ev.event_type, 0) + ev.quantity
        cost += ev.quantity * prices.get(ev.event_type, 0.0)

    return UsageSummary(tenant_id=tenant_id, since=since, counts_by_event_type=counts,
                         estimated_cost_usd=round(cost, 4))


class BillingProvider(ABC):
    """What a real payment processor integration implements. Called at
    whatever cadence a real deployment bills on (e.g. monthly) with a
    UsageSummary for the period."""

    @abstractmethod
    def charge(self, tenant_id: str, summary: UsageSummary) -> dict:
        """Returns a provider-specific result dict (e.g. a Stripe invoice
        ID). Implementations should raise on failure, not swallow it --
        unlike llm_adjudication.py's "fail safe, fall back silently"
        pattern, a billing failure should be loud, since silently not
        charging (or not noticing a charge failed) is a real business
        problem, not something to gracefully degrade past.

        Args:
            tenant_id: Tenant to charge.
            summary: Usage for the billing period, from ``get_usage_summary``.

        Returns:
            dict: Provider-specific result, e.g. an invoice ID.
        """


class LoggingBillingProvider(BillingProvider):
    """The only implementation that actually runs here -- records what
    WOULD be charged rather than charging anything. This is the safe,
    working default; a real deployment swaps this for StripeBillingProvider
    (or similar) once real payment credentials exist. Kept as a class
    (not just record_usage() above) so the swap is a one-line change at
    the call site, not a rewrite."""

    def __init__(self):
        """Start with an empty charge log.

        The log only lives in memory on this object; it is lost when the
        process stops.
        """
        self.charge_log: list[dict] = []

    def charge(self, tenant_id: str, summary: UsageSummary) -> dict:
        """Record what would be charged, without charging anything.

        Appends a record to ``self.charge_log`` and returns it. No money moves
        and nothing is written to the database.

        Args:
            tenant_id: Tenant that would be charged.
            summary: Usage for the period.

        Returns:
            dict: ``tenant_id``, ``charged_at``, ``amount_usd``, ``counts`` and a
            ``note`` saying no real charge was made.
        """
        record = {
            "tenant_id": tenant_id, "charged_at": now_iso(),
            "amount_usd": summary.estimated_cost_usd, "counts": summary.counts_by_event_type,
            "note": "LoggingBillingProvider: no real charge was made -- this is a recorded intent only.",
        }
        self.charge_log.append(record)
        return record

# To use a real processor:
#   class StripeBillingProvider(BillingProvider):
#       def __init__(self, api_key: str): self._stripe = stripe; stripe.api_key = api_key
#       def charge(self, tenant_id, summary):
#           return stripe.InvoiceItem.create(customer=..., amount=int(summary.estimated_cost_usd * 100), ...)
# -- needs `pip install stripe`, a real Stripe account, and network access none of which
# are available in this sandbox, so this is documented rather than implemented here.
