"""
Retrains the active-learning match-scoring model for a (tenant, domain)
pair from accumulated steward confirm/reject decisions.

    python3 retrain.py                                   # default tenant, customer domain
    python3 retrain.py --tenant acme --domain product
    python3 retrain.py --tenant acme --domain vendor

Needs at least active_learning.MIN_TRAINING_EXAMPLES (10) decided match
candidates for that (tenant, domain), or it reports there isn't enough
data yet and leaves any existing model untouched.
"""
import argparse

from app.active_learning import MIN_TRAINING_EXAMPLES, build_training_set, feature_names, retrain
from app.domains import REGISTRY, get_domain_config
from app.store_factory import get_store


def main():
    """Command-line entry point: retrain the match model for one tenant and domain.

    Options: ``--tenant`` (default ``"default"``), ``--domain`` (default
    ``"customer"``) and ``--db`` (SQLite path or Postgres URL; defaults to
    ``DATABASE_URL`` or ``mdm.db``).

    Prints how many confirmed and rejected decisions are available. With
    fewer than ``MIN_TRAINING_EXAMPLES`` (10) it stops without changing any
    saved model. Otherwise it trains, saves the model under ``models/`` in
    the current directory, and prints each learned feature weight and the
    bias. The new model is used automatically by later ingests for that
    tenant and domain.

    Note: the training set is built twice (once for the counts here, once
    inside ``retrain``).
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", default="default", help="Tenant ID to retrain (default: 'default')")
    parser.add_argument("--domain", default="customer", choices=list(REGISTRY),
                         help="Domain to retrain (default: 'customer')")
    parser.add_argument("--db", default=None,
                         help="SQLite file path, or a postgresql:// URL. Defaults to DATABASE_URL "
                              "env var if set, else mdm.db.")
    args = parser.parse_args()

    store = get_store(args.db)
    examples = build_training_set(store, args.tenant, args.domain)
    confirmed = sum(1 for _, label in examples if label == 1)
    rejected = sum(1 for _, label in examples if label == 0)

    print(f"Tenant: {args.tenant} | Domain: {args.domain}")
    print(f"Labeled examples available: {len(examples)} ({confirmed} confirmed, {rejected} rejected)")

    if len(examples) < MIN_TRAINING_EXAMPLES:
        print(f"Need at least {MIN_TRAINING_EXAMPLES} — not retraining. "
              f"Resolve more items in the '{args.domain}' review queue first.")
        return

    model = retrain(store, args.tenant, args.domain)
    print(f"\nTrained on {model.trained_on_examples} examples. "
          f"Saved to models/{args.tenant}__{args.domain}_match_model.json")
    print("Weights:")
    config = get_domain_config(args.domain)
    for name, w in zip(feature_names(config), model.weights):
        print(f"  {name}: {w:+.3f}")
    print(f"  bias: {model.bias:+.3f}")
    print(f"\nThis model will now be blended into '{args.domain}' matching automatically for this tenant.")


if __name__ == "__main__":
    main()
