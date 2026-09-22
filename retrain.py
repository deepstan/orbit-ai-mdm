"""
Retrains the active-learning match-scoring model for a tenant from
accumulated steward confirm/reject decisions.

In production this would run on a schedule (nightly cron job, or a
Temporal/Celery periodic task per the architecture doc) -- run it by hand
for now:

    python3 retrain.py                  # retrains the 'default' tenant
    python3 retrain.py --tenant acme    # retrains a specific tenant

Needs at least active_learning.MIN_TRAINING_EXAMPLES (10) decided match
candidates for that tenant, or it will report that there isn't enough
data yet and leave any existing model untouched.
"""
import argparse

from app.active_learning import MIN_TRAINING_EXAMPLES, build_training_set, retrain
from app.db import Store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", default="default", help="Tenant ID to retrain (default: 'default')")
    parser.add_argument("--db", default="mdm.db", help="Path to the SQLite database (default: mdm.db)")
    args = parser.parse_args()

    store = Store(args.db)
    examples = build_training_set(store, args.tenant)
    confirmed = sum(1 for _, label in examples if label == 1)
    rejected = sum(1 for _, label in examples if label == 0)

    print(f"Tenant: {args.tenant}")
    print(f"Labeled examples available: {len(examples)} ({confirmed} confirmed, {rejected} rejected)")

    if len(examples) < MIN_TRAINING_EXAMPLES:
        print(f"Need at least {MIN_TRAINING_EXAMPLES} — not retraining. "
              f"Resolve more items in the stewardship review queue first.")
        return

    model = retrain(store, args.tenant)
    print(f"\nTrained on {model.trained_on_examples} examples. Saved to models/{args.tenant}_match_model.json")
    print("Weights:")
    from app.active_learning import FEATURE_NAMES
    for name, w in zip(FEATURE_NAMES, model.weights):
        print(f"  {name}: {w:+.3f}")
    print(f"  bias: {model.bias:+.3f}")
    print("\nThis model will now be blended into matching automatically for this tenant.")


if __name__ == "__main__":
    main()
