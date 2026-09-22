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
from app.db import Store
from app.domains import REGISTRY, get_domain_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", default="default", help="Tenant ID to retrain (default: 'default')")
    parser.add_argument("--domain", default="customer", choices=list(REGISTRY),
                         help="Domain to retrain (default: 'customer')")
    parser.add_argument("--db", default="mdm.db", help="Path to the SQLite database (default: mdm.db)")
    args = parser.parse_args()

    store = Store(args.db)
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
