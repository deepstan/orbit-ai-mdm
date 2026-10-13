"""
Semantic similarity layer (Phase 2 item #1).

Jaro-Winkler (in matching.py) catches typos and minor spelling variants,
but it's blind to genuine semantic equivalence: "Bob's Hardware Inc." vs.
"Robert's Hardware Company" score low on string similarity despite almost
certainly being the same business, because the *strings* differ a lot even
though the *meaning* doesn't.

This module provides a pluggable `EmbeddingProvider` interface with two
implementations:

  - `NgramHashEmbedding` — pure Python, zero dependencies, works offline.
    It's a character n-gram bag-of-hashes vector (a lightweight cousin of
    what SentenceTransformers does with a neural encoder). It genuinely
    captures *partial* semantic signal — shared substrings, word order
    insensitivity, robustness to abbreviation — but it is NOT a real
    semantic embedding model. Treat it as a "better than pure string
    distance, worse than a real encoder" fallback. This is what's active
    by default, and what the tests below exercise.

  - `SentenceTransformerEmbedding` — wraps the real `sentence-transformers`
    library (e.g. `all-MiniLM-L6-v2`) for genuine semantic similarity.
    Requires `pip install sentence-transformers` (needs network access to
    download the model on first use) — NOT installed or tested in this
    sandbox. Swap it in by setting EMBEDDING_PROVIDER=sentence_transformers.

Both implementations expose the same `.embed(text) -> vector` and
`.similarity(text_a, text_b) -> float in [0,1]` interface, so
`matching.py` doesn't care which one is active.
"""
from __future__ import annotations

import hashlib
import math
import os
import re
from functools import lru_cache
from typing import Protocol


class EmbeddingProvider(Protocol):
    def embed(self, text: str) -> dict[int, float]: ...
    def similarity(self, text_a: str, text_b: str) -> float: ...


def _ngrams(text: str, n: int = 3) -> list[str]:
    text = re.sub(r"\s+", " ", text.lower().strip())
    padded = f" {text} "
    if len(padded) < n:
        return [padded]
    return [padded[i:i + n] for i in range(len(padded) - n + 1)]


def _embed_impl(text: str, n: int, buckets: int) -> dict[int, float]:
    vec: dict[int, float] = {}
    for gram in _ngrams(text, n):
        h = int(hashlib.md5(gram.encode("utf-8")).hexdigest(), 16) % buckets
        vec[h] = vec.get(h, 0.0) + 1.0
    # L2-normalize so cosine similarity is just a dot product
    norm = math.sqrt(sum(v * v for v in vec.values())) or 1.0
    return {k: v / norm for k, v in vec.items()}


# A pure function of (text, n, buckets), and one of the two largest costs of scoring a pair (an MD5 per trigram). Cached, BOUNDED: an entry is about 2 KB, so the default is about
# 35 MB per process. MDM_EMBEDDING_CACHE_SIZE=0 turns it off. The cached dict is SHARED and read-only: similarity() never mutates it, and embed() hands out copies.
EMBEDDING_CACHE_DEFAULT = 16_384


def _embedding_cache_size() -> int:
    raw = os.environ.get("MDM_EMBEDDING_CACHE_SIZE")
    if raw is None or raw.strip() == "":
        return EMBEDDING_CACHE_DEFAULT
    try:
        n = int(raw)
    except ValueError:
        raise ValueError(f"MDM_EMBEDDING_CACHE_SIZE must be a whole number (0 turns the cache off); got {raw!r}") from None
    if n < 0:
        raise ValueError(f"MDM_EMBEDDING_CACHE_SIZE must not be negative; got {n}")
    return n


_embed_cached = lru_cache(maxsize=_embedding_cache_size())(_embed_impl)


class NgramHashEmbedding:
    """A sparse bag-of-char-trigrams vector, hashed into a fixed number of
    buckets (the 'hashing trick' — avoids needing a fitted vocabulary).
    Cosine similarity over these vectors is tolerant of word reordering,
    abbreviation, and partial overlap in a way raw string distance isn't.
    """

    def __init__(self, n: int = 3, buckets: int = 512):
        self.n = n
        self.buckets = buckets

    def embed(self, text: str) -> dict[int, float]:
        """A fresh dict each call (the caller may do what it likes with it); the work behind it is cached."""
        return dict(_embed_cached(text, self.n, self.buckets))

    def similarity(self, text_a: str, text_b: str) -> float:
        if not text_a or not text_b:
            return 0.0
        va, vb = _embed_cached(text_a, self.n, self.buckets), _embed_cached(text_b, self.n, self.buckets)   # shared, read-only: never mutated here
        # dot product of two sparse, already-normalized vectors
        smaller, larger = (va, vb) if len(va) < len(vb) else (vb, va)
        return max(0.0, min(1.0, sum(v * larger.get(k, 0.0) for k, v in smaller.items())))


class SentenceTransformerEmbedding:
    """Real semantic embeddings via sentence-transformers. Not usable in
    this sandbox (no network to download the model) — provided so the
    swap is a one-line config change once you're running locally.

        pip install sentence-transformers
        export EMBEDDING_PROVIDER=sentence_transformers
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        try:
            from sentence_transformers import SentenceTransformer  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "sentence-transformers is not installed. Run:\n"
                "  pip install sentence-transformers\n"
                "or unset EMBEDDING_PROVIDER to use the offline fallback."
            ) from e
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(model_name)

    def embed(self, text: str) -> dict[int, float]:
        vec = self._model.encode(text, normalize_embeddings=True)
        return {i: float(v) for i, v in enumerate(vec)}

    def similarity(self, text_a: str, text_b: str) -> float:
        if not text_a or not text_b:
            return 0.0
        from sentence_transformers import util
        emb = self._model.encode([text_a, text_b], normalize_embeddings=True)
        return float(util.cos_sim(emb[0], emb[1])[0][0])


def get_provider() -> EmbeddingProvider:
    """Reads EMBEDDING_PROVIDER env var; defaults to the offline fallback
    so the app runs with zero setup. Set EMBEDDING_PROVIDER=sentence_transformers
    once you've `pip install`-ed it for real semantic matching."""
    choice = os.environ.get("EMBEDDING_PROVIDER", "ngram_hash")
    if choice == "sentence_transformers":
        return SentenceTransformerEmbedding()
    return NgramHashEmbedding()
