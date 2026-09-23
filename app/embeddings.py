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
from typing import Protocol


class EmbeddingProvider(Protocol):
    def embed(self, text: str) -> dict[int, float]:
        """Turn text into a vector, returned as ``{dimension_index: value}``.

        Args:
            text: Input text.

        Returns:
            dict[int, float]: The (possibly sparse) embedding vector.
        """
        ...

    def similarity(self, text_a: str, text_b: str) -> float:
        """Return how similar two texts are, normally between 0.0 and 1.0.

        Implementations must return ``0.0`` when either text is empty.

        Args:
            text_a: First text.
            text_b: Second text.

        Returns:
            float: Similarity score (higher means more alike).
        """
        ...


def _ngrams(text: str, n: int = 3) -> list[str]:
    """Split text into overlapping character n-grams (default: trigrams).

    The text is lowercased, whitespace is collapsed, and one space is added
    at each end so word starts and ends get their own n-grams.

    Args:
        text: Input text.
        n: Length of each n-gram. Defaults to 3.

    Returns:
        list[str]: The n-grams in order. If the padded text is shorter than
        ``n``, returns a single-item list containing the padded text.

    Example:
        ``_ngrams("Bob")`` -> ``[" bo", "bob", "ob "]``
    """
    text = re.sub(r"\s+", " ", text.lower().strip())
    padded = f" {text} "
    if len(padded) < n:
        return [padded]
    return [padded[i:i + n] for i in range(len(padded) - n + 1)]


class NgramHashEmbedding:
    """A sparse bag-of-char-trigrams vector, hashed into a fixed number of
    buckets (the 'hashing trick' — avoids needing a fitted vocabulary).
    Cosine similarity over these vectors is tolerant of word reordering,
    abbreviation, and partial overlap in a way raw string distance isn't.
    """

    def __init__(self, n: int = 3, buckets: int = 512):
        """Configure the n-gram size and the number of hash buckets.

        Args:
            n: Character n-gram length. Defaults to 3.
            buckets: Size of the hashed vector space. More buckets means fewer
                unrelated n-grams sharing a bucket (hash collisions) but larger
                vectors. Defaults to 512.
        """
        self.n = n
        self.buckets = buckets

    def embed(self, text: str) -> dict[int, float]:
        """Turn text into a sparse, unit-length n-gram count vector.

        Each n-gram is hashed with MD5 into one of ``self.buckets`` slots and
        counted; the vector is then scaled to length 1 so that cosine
        similarity is a plain dot product. MD5 is used only as a stable hash
        (same result on every run and machine), not for security.

        Args:
            text: Input text.

        Returns:
            dict[int, float]: ``{bucket_index: weight}`` for non-zero buckets.
        """
        vec: dict[int, float] = {}
        for gram in _ngrams(text, self.n):
            h = int(hashlib.md5(gram.encode("utf-8")).hexdigest(), 16) % self.buckets
            vec[h] = vec.get(h, 0.0) + 1.0
        # L2-normalize so cosine similarity is just a dot product
        norm = math.sqrt(sum(v * v for v in vec.values())) or 1.0
        return {k: v / norm for k, v in vec.items()}

    def similarity(self, text_a: str, text_b: str) -> float:
        """Cosine similarity between two texts' n-gram vectors, from 0.0 to 1.0.

        Embeds both texts and takes the dot product, looping over the smaller
        vector for speed. The result is clamped to ``[0, 1]``.

        Args:
            text_a: First text.
            text_b: Second text.

        Returns:
            float: Similarity, or ``0.0`` if either text is empty.
        """
        if not text_a or not text_b:
            return 0.0
        va, vb = self.embed(text_a), self.embed(text_b)
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
        """Load a sentence-transformers model.

        The first call downloads the model from the internet if it isn't
        cached locally.

        Args:
            model_name: Name of the sentence-transformers model. Defaults to
                ``"all-MiniLM-L6-v2"`` (small and fast).

        Raises:
            ImportError: If ``sentence-transformers`` is not installed.
        """
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
        """Encode text into a dense, unit-length embedding.

        Returned as ``{index: value}`` so it has the same shape as
        ``NgramHashEmbedding.embed``, even though every dimension is filled.

        Args:
            text: Input text.

        Returns:
            dict[int, float]: One entry per embedding dimension (384 for the
            default model).
        """
        vec = self._model.encode(text, normalize_embeddings=True)
        return {i: float(v) for i, v in enumerate(vec)}

    def similarity(self, text_a: str, text_b: str) -> float:
        """Semantic (cosine) similarity between two texts using the model.

        Note: cosine similarity from a neural model can be slightly negative for
        unrelated texts. Unlike ``NgramHashEmbedding``, this result is NOT
        clamped to ``[0, 1]``.

        Args:
            text_a: First text.
            text_b: Second text.

        Returns:
            float: Cosine similarity (normally 0-1), or ``0.0`` if either text is
            empty.
        """
        if not text_a or not text_b:
            return 0.0
        from sentence_transformers import util
        emb = self._model.encode([text_a, text_b], normalize_embeddings=True)
        return float(util.cos_sim(emb[0], emb[1])[0][0])


def get_provider() -> EmbeddingProvider:
    """Reads EMBEDDING_PROVIDER env var; defaults to the offline fallback
    so the app runs with zero setup. Set EMBEDDING_PROVIDER=sentence_transformers
    once you've `pip install`-ed it for real semantic matching.

    Any value other than ``"sentence_transformers"`` (including typos)
    silently gives the n-gram fallback. ``matching.py`` caches the result,
    so the variable is only read once per process.

    Returns:
        EmbeddingProvider: ``SentenceTransformerEmbedding`` or
        ``NgramHashEmbedding``.

    Raises:
        ImportError: If ``sentence_transformers`` is requested but not
            installed.
    """
    choice = os.environ.get("EMBEDDING_PROVIDER", "ngram_hash")
    if choice == "sentence_transformers":
        return SentenceTransformerEmbedding()
    return NgramHashEmbedding()
