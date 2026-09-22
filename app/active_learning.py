"""
Active learning loop (Phase 2 item #4) -- architecture doc Section 5.5:

    "Every steward decision ... is logged as a labeled training example.
    Periodic retraining ... of the match-scoring model per tenant ...
    improves match quality over time without manual rule-tuning."

Every `MatchCandidate` in the DB with status "confirmed" or "rejected"
IS a labeled training example (confirmed=1, rejected=0) -- that data has
been accumulating since Phase 1 without anything using it. This module
closes the loop: it recomputes the same feature signals `matching.py`
already produces for each historical pair, trains a small logistic
regression classifier via gradient descent, and persists per-tenant
weights that `matching.py` can optionally blend into its score.

Pure Python (no scikit-learn/numpy -- unavailable in this sandbox), which
also means it's genuinely inspectable: the learned weights are just 7
floats you can print and reason about, not a black box.

Design: this is presented as a *second opinion*, not a replacement for
the hand-tuned rules. `score_pair()` still runs first; if a trained model
exists for the tenant (and has seen enough examples to be meaningful),
its prediction is blended in. This mirrors the architecture doc's
framing of AI as augmenting explainable rules, not replacing them outright.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path

from .db import Store
from .matching import similarity
from .standardize import standardize_customer

FEATURE_NAMES = [
    "email_exact", "phone_exact", "first_name_sim", "last_name_sim",
    "address_sim", "city_sim", "postal_sim",
]

MIN_TRAINING_EXAMPLES = 10  # below this, a learned model isn't trustworthy -- stay on hand-tuned rules
LEARNING_RATE = 0.5
EPOCHS = 500
L2_REG = 0.01


@dataclass
class LearnedModel:
    weights: list[float] = field(default_factory=lambda: [0.0] * len(FEATURE_NAMES))
    bias: float = 0.0
    trained_on_examples: int = 0
    tenant_id: str = ""

    def predict_proba(self, features: list[float]) -> float:
        z = self.bias + sum(w * f for w, f in zip(self.weights, features))
        return 1.0 / (1.0 + math.exp(-max(-60.0, min(60.0, z))))  # clamp to avoid overflow

    def to_dict(self) -> dict:
        return {"weights": self.weights, "bias": self.bias,
                "trained_on_examples": self.trained_on_examples, "tenant_id": self.tenant_id}

    @classmethod
    def from_dict(cls, d: dict) -> "LearnedModel":
        return cls(weights=d["weights"], bias=d["bias"],
                    trained_on_examples=d["trained_on_examples"], tenant_id=d.get("tenant_id", ""))


def extract_features(std_a: dict, std_b: dict) -> list[float]:
    """The same signal set score_pair() looks at, as a flat feature
    vector -- kept in a shared, explicit order (FEATURE_NAMES) so the
    learned weights stay human-interpretable."""
    return [
        1.0 if std_a.get("email") and std_a["email"] == std_b.get("email") else 0.0,
        1.0 if std_a.get("phone") and std_a["phone"] == std_b.get("phone") else 0.0,
        similarity(std_a.get("first_name", ""), std_b.get("first_name", "")),
        similarity(std_a.get("last_name", ""), std_b.get("last_name", "")),
        similarity(std_a.get("address_line1", ""), std_b.get("address_line1", "")),
        similarity(std_a.get("city", ""), std_b.get("city", "")),
        similarity(std_a.get("postal_code", ""), std_b.get("postal_code", "")),
    ]


def _model_path(tenant_id: str) -> Path:
    return Path(f"models/{tenant_id}_match_model.json")


def load_model(tenant_id: str) -> LearnedModel | None:
    path = _model_path(tenant_id)
    if not path.exists():
        return None
    return LearnedModel.from_dict(json.loads(path.read_text()))


def save_model(model: LearnedModel) -> None:
    path = _model_path(model.tenant_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model.to_dict(), indent=2))


def build_training_set(store: Store, tenant_id: str) -> list[tuple[list[float], int]]:
    """Pulls every decided (confirmed/rejected) candidate for a tenant and
    turns it into (feature_vector, label) pairs."""
    examples = []
    for status, label in (("confirmed", 1), ("rejected", 0)):
        for cand in store.list_candidates(tenant_id, status=status):
            rec_a = store.get_source_record(cand.record_a_id, tenant_id)
            rec_b = store.get_source_record(cand.record_b_id, tenant_id)
            if not rec_a or not rec_b:
                continue
            std_a, std_b = standardize_customer(rec_a.attributes), standardize_customer(rec_b.attributes)
            examples.append((extract_features(std_a, std_b), label))
    return examples


def train_logistic_regression(examples: list[tuple[list[float], int]]) -> tuple[list[float], float]:
    """Plain-gradient-descent logistic regression -- no external deps.
    Small enough dataset (steward decisions, not millions of rows) that
    this is entirely adequate; swap for scikit-learn's LogisticRegression
    once you have real data volume and a network connection."""
    n_features = len(FEATURE_NAMES)
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


def retrain(store: Store, tenant_id: str) -> LearnedModel | None:
    """Retrains and persists the match-scoring model for one tenant from
    its accumulated steward decisions. Returns None (and doesn't overwrite
    any existing model) if there isn't enough labeled data yet -- this is
    the guard that keeps an undertrained model from ever being used."""
    examples = build_training_set(store, tenant_id)
    if len(examples) < MIN_TRAINING_EXAMPLES:
        return None

    weights, bias = train_logistic_regression(examples)
    model = LearnedModel(weights=weights, bias=bias, trained_on_examples=len(examples), tenant_id=tenant_id)
    save_model(model)
    return model


def blend_scores(rule_based_score: float, model: LearnedModel | None, features: list[float],
                  model_weight: float = 0.35) -> float:
    """Blends the hand-tuned score_pair() score with the learned model's
    prediction, if a model exists. model_weight controls how much trust
    the learned model gets relative to the hand-tuned rules -- start low
    and increase as the model proves itself against held-out decisions."""
    if model is None:
        return rule_based_score
    learned_score = model.predict_proba(features)
    return (1 - model_weight) * rule_based_score + model_weight * learned_score
