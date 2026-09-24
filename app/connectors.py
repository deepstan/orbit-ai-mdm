"""
Connector marketplace (Phase 3 item #4).

What's real here: a `BaseConnector` interface, and one fully working
implementation -- `CSVConnector` -- that reads a local CSV file and
yields (source_record_id, attributes) pairs ready for `ingest_record()`.
This is genuinely useful (CSV import is a real, common way data enters
an MDM system) and fully testable without any external service.

What's NOT here: real Salesforce/Shopify/SAP/etc. connectors. Those need
live API credentials and network access to an actual account of each
service -- neither available in this sandbox, and not something that
can be meaningfully "written but unverified" the way llm_adjudication.py
or db_postgres.py are, because there's no stable public contract to
write against without hitting a real account (auth flows, pagination,
field mappings all vary and need to be built against the real API).

So instead: the interface below is deliberately the SAME shape a real
connector would use, and CRMConnectorStub shows exactly where a real one
plugs in and what it needs to implement -- a scaffold, not a working
integration.
"""
from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator


class BaseConnector(ABC):
    """A connector's job: yield raw (source_record_id, attributes) pairs
    for one domain, ready to hand to `pipeline.ingest_record()`. Field
    mapping (translating the source system's field names into this
    domain's canonical schema -- e.g. Salesforce's `Email` -> our
    `email`) is the connector's responsibility, not the caller's."""

    source_system: str  # identifies which system these records came from, e.g. "salesforce"

    @abstractmethod
    def fetch_records(self) -> Iterator[tuple[str, dict]]:
        """Yields (source_record_id, attributes) pairs. A generator, not
        a list -- so a connector pulling from a large or paginated
        source can stream records rather than loading everything into
        memory first.

        Returns:
            Iterator[tuple[str, dict]]: ``(source_record_id, attributes)``
            pairs, where ``attributes`` already uses the domain's canonical
            field names.
        """


class CSVConnector(BaseConnector):
    """Reads a local CSV file, mapping its columns to a domain's
    canonical field names via `field_mapping`. This is the one connector
    that's fully real and tested -- CSV/flat-file import is one of the
    source types the architecture doc lists (Section 3), and it needs no
    external service or credentials."""

    def __init__(self, csv_path: str, field_mapping: dict[str, str],
                 source_record_id_column: str | None = None, source_system: str = "csv_import"):
        """Configure a CSV import.

        Args:
            csv_path: Path to the CSV file. Read as UTF-8; the first row must be
                the header.
            field_mapping: ``{csv_column_name: canonical_field_name}``, e.g.
                ``{"Email Address": "email", "First": "first_name"}``. Only
                mapped columns are imported.
            source_record_id_column: CSV column to use as the stable
                ``source_record_id``. If omitted, the data row number (1 for the
                first row after the header) is used. That's fine for a one-off
                import, but a recurring feed should give a real stable ID column
                so re-running the import doesn't create duplicate source
                records.
            source_system: Name recorded as the records' source system.
                Defaults to ``"csv_import"``.
        """
        self.csv_path = csv_path
        self.field_mapping = field_mapping
        self.source_record_id_column = source_record_id_column
        self.source_system = source_system

    def fetch_records(self) -> Iterator[tuple[str, dict]]:
        """Read the CSV file and yield one mapped record per row.

        Each mapped column's value is trimmed of surrounding whitespace. A
        mapped column that doesn't exist in the file gives ``""``. The file is
        read lazily, row by row, as the generator is consumed.

        Yields:
            tuple[str, dict]: ``(source_record_id, attributes)`` for each row.

        Raises:
            FileNotFoundError: If ``csv_path`` doesn't exist (raised when
                iteration starts, not when the connector is created).
            KeyError: If ``source_record_id_column`` isn't a column in the file.
            AttributeError: If a row has fewer cells than the header and a
                mapped column is missing from that row.
        """
        path = Path(self.csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=1):
                record_id = row[self.source_record_id_column] if self.source_record_id_column else str(i)
                attributes = {
                    canonical_field: row.get(csv_column, "").strip()
                    for csv_column, canonical_field in self.field_mapping.items()
                }
                yield record_id, attributes


class JSONConnector(BaseConnector):
    """Reads a local JSON file containing an array of objects (the
    common shape for an API export or a data dump), mapping each
    object's keys to canonical field names the same way CSVConnector
    maps columns. Supports dotted paths for nested fields (e.g.
    "contact.email" reaches into {"contact": {"email": "..."}})."""

    def __init__(self, json_path: str, field_mapping: dict[str, str],
                 source_record_id_field: str | None = None, source_system: str = "json_import"):
        """
        json_path: path to a JSON file whose top level is an array of objects.
        field_mapping: {json_field_path: canonical_field_name}, e.g.
            {"contact.email": "email", "given_name": "first_name"}
        source_record_id_field: which field to use as source_record_id
            (dotted paths supported same as field_mapping). If omitted,
            uses the item's 0-based index in the array.
        """
        self.json_path = json_path
        self.field_mapping = field_mapping
        self.source_record_id_field = source_record_id_field
        self.source_system = source_system

    @staticmethod
    def _get_path(obj: dict, dotted_path: str):
        value = obj
        for part in dotted_path.split("."):
            if not isinstance(value, dict):
                return None
            value = value.get(part)
        return value

    def fetch_records(self) -> Iterator[tuple[str, dict]]:
        import json

        path = Path(self.json_path)
        if not path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.json_path}")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError(f"{self.json_path}: expected a top-level JSON array of objects, "
                              f"got {type(data).__name__}")

        for i, item in enumerate(data):
            record_id = (
                str(self._get_path(item, self.source_record_id_field))
                if self.source_record_id_field else str(i)
            )
            attributes = {}
            for json_path_key, canonical_field in self.field_mapping.items():
                value = self._get_path(item, json_path_key)
                attributes[canonical_field] = str(value).strip() if value is not None else ""
            yield record_id, attributes

def ingest_from_connector(store, tenant_id: str, domain: str, connector: BaseConnector) -> list[dict]:
    """Convenience wrapper: pulls every record from a connector and runs
    it through the normal ingest pipeline. Returns the list of
    ingest_record() results, one per record.

    Records are ingested one at a time, in order; each one can match
    records ingested earlier in the same run. If one record fails, the
    exception stops the import and records already ingested stay saved.

    Args:
        store: Storage backend.
        tenant_id: Tenant to ingest into.
        domain: Domain of the records.
        connector: Any ``BaseConnector`` implementation.

    Returns:
        list[dict]: One ``ingest_record`` result per record.
    """
    from .pipeline import ingest_record  # local import avoids a module-load-order cycle

    results = []
    for source_record_id, attributes in connector.fetch_records():
        result = ingest_record(store, tenant_id, domain, connector.source_system, source_record_id, attributes)
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# NOT IMPLEMENTED -- scaffolds only, shown so a real integration has a clear
# shape to fill in. Each needs: a real account of the service, an API
# client library or raw HTTP calls, an auth flow (OAuth for most of
# these), pagination handling, and a field-mapping table like
# CSVConnector's -- none of which can be built against a real API from
# this sandbox.
# ---------------------------------------------------------------------------
class SalesforceConnectorStub(BaseConnector):
    source_system = "salesforce"

    def __init__(self, *_args, **_kwargs):
        """Always raises: this is a placeholder for a future Salesforce connector.

        Raises:
            NotImplementedError: Always, with a note on what a real
                implementation needs (a Salesforce org, OAuth credentials and a
                client library).
        """
        raise NotImplementedError(
            "Real Salesforce integration needs a live Salesforce account, OAuth credentials, "
            "and the simple-salesforce (or similar) library. This class documents the shape "
            "(BaseConnector.fetch_records()) but isn't implemented -- build it against your "
            "actual Salesforce org's field schema, which this sandbox has no access to."
        )

    def fetch_records(self):
        """Not implemented; the connector can't be created in the first place.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError


class ShopifyConnectorStub(BaseConnector):
    source_system = "shopify"

    def __init__(self, *_args, **_kwargs):
        """Always raises: this is a placeholder for a future Shopify connector.

        Raises:
            NotImplementedError: Always, with a note on what a real
                implementation needs (an Admin API access token and network
                access to the store).
        """
        raise NotImplementedError(
            "Real Shopify integration needs a live store's Admin API access token and network "
            "access to https://<store>.myshopify.com/admin/api/... -- same shape as CSVConnector "
            "(BaseConnector.fetch_records()), not implemented here for the same reason."
        )

    def fetch_records(self):
        """Not implemented; the connector can't be created in the first place.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError
