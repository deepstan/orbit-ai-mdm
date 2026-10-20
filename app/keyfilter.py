"""
THE KEY FILTER: skip the lookups that would find nothing (docs/kernel-design.md, section 7c, B2).

Measured on 4,000 people: 92% of records share NONE of their 14 blocking keys with any other record, so both blocking lookups for them (bucket sizes, then members) find only the record itself.
A read cache cannot skip those lookups, because "nothing is there" can only be answered by something that holds the WHOLE index, and at 100 million records that is 1.4 billion keys.
A Bloom filter can: it answers "definitely never seen" or "maybe seen" in about ten bits per key.

  * "definitely never seen"  -> the key is held by no other record: nothing to look up
  * "maybe seen"             -> ask the database, exactly as before

A wrong "maybe" costs one lookup and changes no result. A wrong "definitely never" would lose a candidate, and that cannot happen as long as these three things hold, so they are the contract:
  1. ONE WRITER. Nothing else may add keys to this tenant and domain while the filter is in use (no API traffic, no second loader, no second worker): a key added behind its back would be
     "definitely never seen" when it exists. The bulk loader asks for it explicitly (--key-filter), and it is off by default.
  2. IT STARTS COMPLETE. It is loaded from every key already in the index (`load_existing`) before the first record, and gives up (the load then runs unfiltered) if there are too many to hold.
  3. EVERY key of every record is added, in the order the records are matched: a record's keys are checked BEFORE they are added, so it sees only other records'.
Keys that are later deleted (a record changed or left) stay in the filter: a Bloom filter cannot forget, and a stale "maybe" is harmless.

When the filter fills up its false-"maybe" rate climbs; past a threshold it stops filtering altogether (every key is a "maybe"), which is slow but never wrong.
"""
from __future__ import annotations

import hashlib
import logging

logger = logging.getLogger("mdm.keyfilter")

DEFAULT_BITS_PER_KEY = 10        # about 1% false "maybe" with 7 hashes
HASHES = 7
GIVE_UP_FILL = 0.85              # the share of bits set at which it is worth less than it costs


class KeyFilterTooBig(Exception):
    """There are more keys in the index than the filter was allowed to scan."""


class KeyFilter:
    def __init__(self, expected_keys: int, bits_per_key: int = DEFAULT_BITS_PER_KEY):
        self.bits = max(4096, int(expected_keys) * int(bits_per_key))
        self.bits += (-self.bits) % 8
        self._array = bytearray(self.bits // 8)
        self._set = 0
        self.asked = self.maybe = self.added = 0
        self._gave_up = False

    # ---- the bits
    def _positions(self, key: str):
        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=16).digest()
        h1, h2 = int.from_bytes(digest[:8], "little"), int.from_bytes(digest[8:], "little") | 1       # double hashing: h1 + i*h2 gives HASHES independent-enough positions
        return [(h1 + i * h2) % self.bits for i in range(HASHES)]

    def _present_and_set(self, key: str, set_bits: bool) -> bool:
        present = True
        for p in self._positions(key):
            byte, mask = p >> 3, 1 << (p & 7)
            if not self._array[byte] & mask:
                present = False
                if set_bits:
                    self._array[byte] |= mask
                    self._set += 1
        return present

    # ---- using it
    @property
    def fill(self) -> float:
        return self._set / self.bits

    @property
    def giving_up(self) -> bool:
        if not self._gave_up and self.fill > GIVE_UP_FILL:
            self._gave_up = True
            logger.warning("the key filter is %.0f%% full and no longer filters (every key is looked up, as without it): it was sized too small for this load", self.fill * 100)
        return self._gave_up

    def add(self, keys) -> None:
        for k in keys:
            self._present_and_set(k, True)
            self.added += 1

    def check_and_add(self, keys) -> set:
        """The keys that MAY be held by another record, and registers all of them as held by this one. The check comes first, so a record never sees its own keys."""
        maybe = set()
        for k in keys:
            present = self._present_and_set(k, True)
            self.asked += 1
            self.added += 1
            if present:
                maybe.add(k)
        if self.giving_up:
            return set(keys)
        self.maybe += len(maybe)
        return maybe

    def load_existing(self, store, tenant_id: str, domain: str, max_keys: int, page: int = 50_000) -> int:
        """Everything already in the index (the contract's rule 2). Raises KeyFilterTooBig past max_keys, and the caller then runs without the filter."""
        total = 0
        for keys in store.iter_blocking_keys(tenant_id, domain, page):
            total += len(keys)
            if total > max_keys:
                raise KeyFilterTooBig(f"more than {max_keys:,} keys are already in the index")
            self.add(keys)
        return total

    def summary(self) -> str:
        skipped = self.asked - self.maybe
        return (f"key filter: {self.asked:,} keys of new records checked, {skipped:,} ({skipped / self.asked:.0%}) known to be held by no other record and not looked up; "
                f"{self.fill:.0%} full" if self.asked else "key filter: no new record was checked")


KEYS_PER_RECORD_ESTIMATE = 20       # a person has about 14, a customer more: sized generously, because a filter that fills up stops helping


def build_for_load(store, tenant_id: str, domain: str, expected_new_records: int, max_existing_keys: int = 200_000_000, bits_per_key: int = DEFAULT_BITS_PER_KEY):
    """(filter, message). A filter sized for what is already indexed plus the records about to be loaded, and loaded with every key already in the index. (None, why) when that is not
    possible (too many keys already there): the load then runs without it, slower and exactly as correct."""
    existing = store.count_blocking_keys(tenant_id, domain)
    if existing > max_existing_keys:
        return None, f"the key filter was not used: {existing:,} keys are already in the index, more than the {max_existing_keys:,} it may scan (--key-filter-max-keys)"
    kf = KeyFilter(existing + expected_new_records * KEYS_PER_RECORD_ESTIMATE, bits_per_key)
    try:
        loaded = kf.load_existing(store, tenant_id, domain, max_existing_keys)
    except KeyFilterTooBig as e:
        return None, f"the key filter was not used: {e}"
    return kf, f"key filter ON: {loaded:,} keys already in the index loaded; {kf.bits / 8 / 1_048_576:,.0f} MB for {expected_new_records:,} new records"
