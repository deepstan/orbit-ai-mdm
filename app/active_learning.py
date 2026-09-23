"""
Active learning loop -- domain-agnostic (Phase 2 item #4 + Product domain).

Every `MatchCandidate` in the DB with status "confirmed" or "rejected" IS
a labeled training example (confirmed=1, rejected=0). This module closes
the loop: it recomputes the same feature signals `matching.py` already
produces for each historical pair -- now derived from whatever
DomainConfig applies, not hardcoded customer field names -- trains a
small logistic regression classifier via gradient descent, and persists
per-(tenant, domain) weights that `matching.py`'s caller can optionally
blend in.

Pure Python (no scikit-learn/numpy), fully inspectable: the learned
weights are just N floats you can print and reason about.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path

from .db import Store
from .domains import DomainConfig, get_domain_config
from .matching import similarity
from .standardize import standardize

MIN_TRAINING_EXAMPLES = 10
LEARNING_RATE = 0.5
EPOCHS = 500
L2_REG = 0.01


def feature_names(config: DomainConfig) -> list[str]:
    """Feature vector layout for a domain: one boolean per deterministic
    field, one similarity per fuzzy-weighted field, plus a semantic slot
    if the domain uses one. Order matters -- extract_features() below
    must produce values in this same order.

    Args:
        config: The domain's configuration.

    Returns:
        list[str]: Feature names, e.g. for customer:
        ``["email_exact", "phone_exact", "first_name_sim", "last_name_sim",
        "address_line1_sim", "city_sim", "postal_code_sim", "semantic_sim"]``.
    """
    names = [f"{f}_exact" for f in config.deterministic_fields]
    names += [f"{f}_sim" for f in config.fuzzy_weights]
    if config.semantic_fields:
        names.append("semantic_sim")
    return names


@dataclass
class LearnedModel:
    weights: list[float] = field(default_factory=list)
    bias: float = 0.0
    trained_on_examples: int = 0
    tenant_id: str = ""
    domain: str = ""

    def predict_proba(self, features: list[float]) -> float:
        """Predict the probability that a pair is a real match.

        Computes ``bias + sum(weight * feature)`` and passes it through the
        sigmoid function. The raw value is clamped to ``[-60, 60]`` first to
        avoid overflow in ``math.exp``.

        If the feature list and weight list have different lengths (for example
        because the domain config changed after training), ``zip`` silently
        ignores the extra items. Retrain after changing a domain's fields.

        Args:
            features: Feature vector from ``extract_features``.

        Returns:
            float: Match probability between 0 and 1.
        """
        z = self.bias + sum(w * f for w, f in zip(self.weights, features))
        return 1.0 / (1.0 + math.exp(-max(-60.0, min(60.0, z))))

    def to_dict(self) -> dict:
        """Convert the model to a plain dict for saving as JSON.

        Returns:
            dict: ``weights``, ``bias``, ``trained_on_examples``, ``tenant_id``
            and ``domain``.
        """
        return {"weights": self.weights, "bias": self.bias, "trained_on_examples": self.trained_on_examples,
                "tenant_id": self.tenant_id, "domain": self.domain}

    @classmethod
    def from_dict(cls, d: dict) -> "LearnedModel":
        """Rebuild a model from a dict produced by ``to_dict``.

        ``tenant_id`` and ``domain`` are optional (older saved models may not
        have them) and default to ``""``.

        Args:
            d: The saved dict.

        Returns:
            LearnedModel: The restored model.

        Raises:
            KeyError: If ``weights``, ``bias`` or ``trained_on_examples`` is missing.
        """
        return cls(weights=d["weights"], bias=d["bias"], trained_on_examples=d["trained_on_examples"],
                    tenant_id=d.get("tenant_id", ""), domain=d.get("domain", ""))


def extract_features(std_a: dict, std_b: dict, config: DomainConfig) -> list[float]:
    """The same signal set score_pair() looks at, as a flat feature
    vector, in the exact order feature_names() declares.

    Values:

    - ``<field>_exact``: 1.0 if both sides have the same non-empty value,
      else 0.0 (one per deterministic field).
    - ``<field>_sim``: Jaro-Winkler similarity, 0.0 if either side is empty
      (one per fuzzy-weighted field).
    - ``semantic_sim``: embedding similarity of the joined semantic fields,
      0.0 if either side is empty (only if the domain has semantic fields).

    Args:
        std_a: Standardized attributes of the first record.
        std_b: Standardized attributes of the second record.
        config: The domain's configuration.

    Returns:
        list[float]: One value per name in ``feature_names(config)``.
    """
    features = []
    for f in config.deterministic_fields:
        val_a = std_a.get(f)
        features.append(1.0 if val_a and val_a == std_b.get(f) else 0.0)
    for f in config.fuzzy_weights:
        features.append(similarity(std_a.get(f, ""), std_b.get(f, "")))
    if config.semantic_fields:
        text_a = " ".join(std_a.get(f, "") for f in config.semantic_fields).strip()
        text_b = " ".join(std_b.get(f, "") for f in config.semantic_fields).strip()
        if text_a and text_b:
            from .embeddings import get_provider
            features.append(get_provider().similarity(text_a, text_b))
        else:
            features.append(0.0)
    return features


def _model_path(tenant_id: str, domain: str) -> Path:
    """File path where the model for one tenant and domain is saved.

    The path is relative to the current working directory:
    ``models/<tenant_id>__<domain>_match_model.json``. Running the app from a
    different folder means a different ``models/`` folder is used.

    Args:
        tenant_id: Tenant ID.
        domain: Domain name.

    Returns:
        Path: The JSON file path (it may not exist yet).
    """
    return Path(f"models/{tenant_id}__{domain}_match_model.json")


def load_model(tenant_id: str, domain: str) -> LearnedModel | None:
    """Load the saved match model for a tenant and domain, if there is one.

    Args:
        tenant_id: Tenant ID.
        domain: Domain name.

    Returns:
        LearnedModel | None: The model, or ``None`` if none has been trained
        yet.
    """
    path = _model_path(tenant_id, domain)
    if not path.exists():
        return None
    return LearnedModel.from_dict(json.loads(path.read_text()))


def save_model(model: LearnedModel) -> None:
    """Save a model to its JSON file, overwriting any previous version.

    Creates the ``models/`` folder if needed. The file location comes from
    the model's own ``tenant_id`` and ``domain``.

    Args:
        model: The trained model to save.
    """
    path = _model_path(model.tenant_id, model.domain)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model.to_dict(), indent=2))


def build_training_set(store: Store, tenant_id: str, domain: str) -> list[tuple[list[float], int]]:
    """Turn decided match candidates into labeled training examples.

    Every candidate with status ``"confirmed"`` becomes a positive example
    (label 1) and every ``"rejected"`` one a negative example (label 0). Both
    source records are re-standardized and turned into features. Pairs where
    either record no longer exists (e.g. purged) are skipped.

    Note: candidates confirmed or rejected automatically (auto-merge or LLM
    decisions) are included too, not only human steward decisions, so the
    model also learns from the system's own past choices.

    Args:
        store: Storage backend.
        tenant_id: Tenant whose decisions to use.
        domain: Domain whose decisions to use.

    Returns:
        list[tuple[list[float], int]]: ``(features, label)`` pairs.
    """
    config = get_domain_config(domain)
    examples = []
    for status, label in (("confirmed", 1), ("rejected", 0)):
        for cand in store.list_candidates(tenant_id, domain, status=status):
            rec_a = store.get_source_record(cand.record_a_id, tenant_id, domain)
            rec_b = store.get_source_record(cand.record_b_id, tenant_id, domain)
            if not rec_a or not rec_b:
                continue
            std_a, std_b = standardize(domain, rec_a.attributes), standardize(domain, rec_b.attributes)
            examples.append((extract_features(std_a, std_b, config), label))
    return examples


def train_logistic_regression(examples: list[tuple[list[float], int]], n_features: int) -> tuple[list[float], float]:
    """Plain-gradient-descent logistic regression -- no external deps.

    Runs ``EPOCHS`` (500) full passes over the data. Each pass computes the
    average gradient of the log-loss, then updates the weights with
    learning rate ``LEARNING_RATE`` (0.5) and L2 regularization ``L2_REG``
    (0.01, which keeps weights small). The bias is not regularized. All
    weights start at 0.

    Args:
        examples: ``(features, label)`` pairs; labels are 0 or 1. Must not be
            empty (an empty list causes a division by zero).
        n_features: Length of each feature vector.

    Returns:
        tuple[list[float], float]: The learned weights and bias.
    """
    weights = [0.0] * n_features
    bias = 0.0
    n = len(examples)

    for _ in range(EPOCHS):
        grad_w = [0.0] * n_features
        grad_b = 0.0
        for features, label in examples:
            z = bias + sum(w * f for w, f in zip(weights, features))
            pred = 1.0 / (1.0 + math.exp(-max(-60.0, min(60.0, z))))
            error = pred - label
            for i, f in enumerate(features):
                grad_w[i] += error * f
            grad_b += error
        for i in range(n_features):
            weights[i] -= LEARNING_RATE * (grad_w[i] / n + L2_REG * weights[i])
        bias -= LEARNING_RATE * (grad_b / n)

    return weights, bias


def retrain(store: Store, tenant_id: str, domain: str) -> LearnedModel | None:
    """Retrains and persists the match-scoring model for one (tenant,
    domain) pair. Returns None (and doesn't overwrite any existing model)
    if there isn't enough labeled data yet.

    Needs at least ``MIN_TRAINING_EXAMPLES`` (10) decided candidates.
    Once saved, the model is picked up by the next ``pipeline.ingest_record``
    call for this tenant and domain.

    Args:
        store: Storage backend.
        tenant_id: Tenant to train for.
        domain: Domain to train for.

    Returns:
        LearnedModel | None: The new model, or ``None`` if there's not
        enough data.

    Raises:
        KeyError: If the domain is unknown.
    """
    config = get_domain_config(domain)
    examples = build_training_set(store, tenant_id, domain)
    if len(examples) < MIN_TRAINING_EXAMPLES:
        return None

    n_features = len(feature_names(config))
    weights, bias = train_logistic_regression(examples, n_features)
    model = LearnedModel(weights=weights, bias=bias, trained_on_examples=len(examples),
                          tenant_id=tenant_id, domain=domain)
    save_model(model)
    return model


def blend_scores(rule_based_score: float, model: LearnedModel | None, features: list[float],
                  model_weight: float = 0.35) -> float:
    """Blends the hand-tuned score_pair() score with the learned model's
    prediction, if a model exists.

    Formula: ``(1 - model_weight) * rule_based_score + model_weight *
    learned_probability``.

    This also applies to deterministic matches (score 0.97). With the
    default weight, a deterministic match stays at or above the auto-merge
    threshold (0.92) only if the model gives it at least ~0.83, so a model
    that disagrees can push an exact email/SKU match down into review.

    Args:
        rule_based_score: Score from ``matching.score_pair``.
        model: The learned model, or ``None``.
        features: Feature vector from ``extract_features`` for the same pair.
        model_weight: Share of the final score that comes from the model.
            Defaults to 0.35.

    Returns:
        float: The blended score, or ``rule_based_score`` unchanged if there
        is no model.
    """
    if model is None:
        return rule_based_score
    learned_score = model.predict_proba(features)
    return (1 - model_weight) * rule_based_score + model_weight * learned_score
