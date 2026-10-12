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
    must produce values in this same order."""
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
        z = self.bias + sum(w * f for w, f in zip(self.weights, features))
        return 1.0 / (1.0 + math.exp(-max(-60.0, min(60.0, z))))

    def to_dict(self) -> dict:
        return {"weights": self.weights, "bias": self.bias, "trained_on_examples": self.trained_on_examples,
                "tenant_id": self.tenant_id, "domain": self.domain}

    @classmethod
    def from_dict(cls, d: dict) -> "LearnedModel":
        return cls(weights=d["weights"], bias=d["bias"], trained_on_examples=d["trained_on_examples"],
                    tenant_id=d.get("tenant_id", ""), domain=d.get("domain", ""))


def extract_features(std_a: dict, std_b: dict, config: DomainConfig) -> list[float]:
    """The same signal set score_pair() looks at, as a flat feature
    vector, in the exact order feature_names() declares."""
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
    return Path(f"models/{tenant_id}__{domain}_match_model.json")


def load_model(tenant_id: str, domain: str) -> LearnedModel | None:
    path = _model_path(tenant_id, domain)
    if not path.exists():
        return None
    return LearnedModel.from_dict(json.loads(path.read_text()))


def save_model(model: LearnedModel) -> None:
    path = _model_path(model.tenant_id, model.domain)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model.to_dict(), indent=2))


def build_training_set(store: Store, tenant_id: str, domain: str) -> list[tuple[list[float], int]]:
    """Only HUMAN labels: a steward's confirm or reject, and the verdict of a person who checked an automatic merge (see Store.human_labeled_candidates).
    The system's own decisions are not labels. This used to take every `confirmed` pair, and an automatic merge is stored as `confirmed`, so the model was
    trained mostly on its own merges (and its only negatives were the review band's rejections)."""
    config = get_domain_config(domain)
    examples = []
    for cand, label, _source in store.human_labeled_candidates(tenant_id, domain):
        rec_a = store.get_source_record(cand.record_a_id, tenant_id, domain)
        rec_b = store.get_source_record(cand.record_b_id, tenant_id, domain)
        if not rec_a or not rec_b:
            continue
        std_a, std_b = standardize(domain, rec_a.attributes), standardize(domain, rec_b.attributes)
        examples.append((extract_features(std_a, std_b, config), label))
    return examples


def train_logistic_regression(examples: list[tuple[list[float], int]], n_features: int) -> tuple[list[float], float]:
    """Plain-gradient-descent logistic regression -- no external deps."""
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
    if there isn't enough labeled data yet."""
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
    prediction, if a model exists."""
    if model is None:
        return rule_based_score
    learned_score = model.predict_proba(features)
    return (1 - model_weight) * rule_based_score + model_weight * learned_score
