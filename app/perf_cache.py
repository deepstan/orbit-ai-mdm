"""
The two bounded caches that make scoring cheaper: string similarity (app/matching.py) and the n-gram text embedding (app/embeddings.py). Both wrap PURE functions, so a cache can change
speed and nothing else (tests/test_caches.py proves identical results, on and off).

    MDM_SIMILARITY_CACHE_SIZE   entries; default 131072 (about 35 MB per process); 0 turns it off
    MDM_EMBEDDING_CACHE_SIZE    entries; default 16384  (about 35 MB per process); 0 turns it off

A bulk run with worker processes holds one of each cache per process. stats() is for operations (is the hit rate high? is the cache too small for the data?); configure() is for tests and tools.
"""
from __future__ import annotations

from functools import lru_cache

from . import embeddings, matching


def stats() -> dict:
    out = {}
    for name, fn in (("similarity", matching._jw), ("embedding", embeddings._embed_cached)):
        info = fn.cache_info()
        calls = info.hits + info.misses
        out[name] = {"hits": info.hits, "misses": info.misses, "size": info.currsize, "max_size": info.maxsize, "hit_rate": (info.hits / calls) if calls else None}
    return out


def clear() -> None:
    matching._jw.cache_clear()
    embeddings._embed_cached.cache_clear()


def configure(similarity: int | None = None, embedding: int | None = None) -> None:
    """Replace a cache by one of another size (an empty one). None leaves that cache alone; 0 turns it off."""
    if similarity is not None:
        matching._jw = lru_cache(maxsize=similarity)(matching.jaro_winkler)
    if embedding is not None:
        embeddings._embed_cached = lru_cache(maxsize=embedding)(embeddings._embed_impl)
