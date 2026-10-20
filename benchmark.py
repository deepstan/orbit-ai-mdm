"""
Measure the matcher against labeled synthetic data (see app/benchmark.py for what the scenarios are
and what the numbers do and don't mean).

    python3 benchmark.py run --seeds 1 2 3 --scale 25 --label baseline --out benchmarks/baseline.json
    python3 benchmark.py compare benchmarks/baseline.json benchmarks/after.json

SSN and date of birth (the `person` profile), and how blocking behaves as the data grows:

    python3 benchmark.py run --profile person --variant full --seeds 1 2 3 --scale 25
    python3 benchmark.py run --profile person --variant ssn_no_phone      # exact SSN + fuzzy name/address, no phone keys
    python3 benchmark.py run --profile person --variant dob_phone_no_ssn  # fuzzy name + DOB + address + phone, no SSN key
    python3 benchmark.py scale --sizes 10000 100000 1000000 --out benchmarks/scale_person.json

Check that every engine (realtime, batch, accept-then-process) gives the SAME entities and review pairs, in any arrival
order, as a database-free reference. Any new, faster engine must pass this before it is trusted:

    python3 benchmark.py equivalence --profile customer --scale 4 --orders 3
    python3 benchmark.py equivalence --profile person --variant full --scale 3

Use --domains-dir to benchmark a variant of a domain config (e.g. different blocking) without changing code.

Good practice: tune on some seeds, then REPORT on seeds you never looked at while tuning.
"""
import argparse
import sys

from app import benchmark
from app.domain_config import DomainConfigError, load_configured_domains


def _scale(benchmark_person, args, base):
    return benchmark_person.scale_profile(
        args.sizes, base=base, seed=args.seed, sample=args.sample, dup_sample=args.dup_sample, cap=args.cap, dup_rate=args.dup_rate,
        variants=args.variants, workers=args.workers, checkpoint=args.checkpoint, budget=args.budget,
        rates={"ssn_rate": args.ssn_rate, "dob_rate": args.dob_rate, "phone_rate": args.phone_rate, "email_rate": args.email_rate})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Generate data, run the real pipeline, report")
    run.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    run.add_argument("--scale", type=int, default=25, help="cases per scenario (background is 10x this)")
    run.add_argument("--label", default="run")
    run.add_argument("--out", default=None, help="save results as JSON")
    run.add_argument("--domains-dir", default=None, help="load domain config overrides from here")
    run.add_argument("--profile", default="customer", choices=["customer", "person"])
    run.add_argument("--variant", default=None, help="person profile only: which blocking keys are live (see app/benchmark_person.VARIANTS)")
    eq = sub.add_parser("equivalence", help="Check every engine against the offline reference, in several arrival orders")
    eq.add_argument("--profile", default="customer", choices=["customer", "person"])
    eq.add_argument("--variant", default=None)
    eq.add_argument("--scale", type=int, default=4)
    eq.add_argument("--seed", type=int, default=1)
    eq.add_argument("--orders", type=int, default=3, help="arrival orders to try: the generated one, then shuffles")
    en = sub.add_parser("engines", help="The SAME generated people through the regular engine and the bulk engine: speed, identical results, and quality against the truth (resumable)")
    en.add_argument("--size", type=int, default=20000)
    en.add_argument("--workdir", required=True)
    en.add_argument("--budget", type=float, default=None, help="regular engine: stop cleanly after about this many seconds per call (exit code 3); run again to continue")
    en.add_argument("--chunk", type=int, default=200)
    en.add_argument("--seed", type=int, default=7)
    en.add_argument("--out", default=None)
    dl = sub.add_parser("delta", help="Measure a DELTA of N records onto M existing people (resumable, in pieces that each fit in one call)")
    dl.add_argument("--base", type=int, default=500000)
    dl.add_argument("--feed", type=int, default=500000)
    dl.add_argument("--piece", type=int, default=50000)
    dl.add_argument("--delta-piece", type=int, default=None, help="the size of the feed slices; may be made smaller between calls (the cost of a slice grows with the data held)")
    dl.add_argument("--workdir", required=True)
    dl.add_argument("--budget", type=float, default=None, help="stop cleanly between pieces after about this many seconds (exit code 3); run again to continue")
    dl.add_argument("--workers", type=int, default=1)
    dl.add_argument("--out", default=None)
    bk = sub.add_parser("bulk", help="Run the bulk engine on N generated people and score it against the truth (resumable)")
    bk.add_argument("--size", type=int, default=100000)
    bk.add_argument("--workdir", required=True, help="where its work files and progress live (needs disk: about 3 GB per million records)")
    bk.add_argument("--budget", type=float, default=None, help="stop cleanly after about this many seconds (exit code 3); run the same command again to continue")
    bk.add_argument("--workers", type=int, default=1)
    bk.add_argument("--chunk", type=int, default=2000)
    bk.add_argument("--seed", type=int, default=7)
    bk.add_argument("--write", action="store_true", help="also run the write stage (default: a dry run of stages A to D)")
    bk.add_argument("--variant", default="scalable_tight_plus")
    bk.add_argument("--before", default=None, help="a scale report to compare with (benchmarks/scale_person_step4b.json)")
    bk.add_argument("--out", default=None)
    sc = sub.add_parser("scale", help="How many records does a record share a blocking key with, as the data grows? (no database)")
    sc.add_argument("--sizes", type=int, nargs="+", default=[10000, 100000])
    sc.add_argument("--seed", type=int, default=7)
    sc.add_argument("--sample", type=int, default=2000, help="records sampled per size (the measurement is exact for the sample)")
    sc.add_argument("--dup-sample", type=int, default=1000, help="duplicates sampled per size, for blocking recall")
    sc.add_argument("--cap", type=int, default=500, help="keys shared by more records than this are not used (max_bucket_size)")
    sc.add_argument("--dup-rate", type=float, default=0.08, help="share of records that are a corrupted copy of an earlier person")
    sc.add_argument("--ssn-rate", type=float, default=0.7, help="share of records that carry an SSN")
    sc.add_argument("--dob-rate", type=float, default=0.9)
    sc.add_argument("--phone-rate", type=float, default=0.8)
    sc.add_argument("--email-rate", type=float, default=0.6)
    sc.add_argument("--variants", nargs="+", default=None)
    sc.add_argument("--workers", type=int, default=1, help="processes (use your core count on a big machine)")
    sc.add_argument("--out", default=None)
    sc.add_argument("--budget", type=float, default=None, help="stop after about this many seconds, saving progress to --checkpoint; run the same command again to continue")
    sc.add_argument("--checkpoint", default=None, help="a file where an interrupted measurement keeps its progress")
    sc.add_argument("--extend", default=None, help="an earlier scale report: measure only the sizes it lacks, and write the combined report back")
    cmp_ = sub.add_parser("compare", help="Compare two saved runs")
    cmp_.add_argument("before")
    cmp_.add_argument("after")
    args = parser.parse_args()

    if args.command in ("engines", "equivalence"):
        from app import kernel
        if kernel.engine() == "kernel":
            print(f"`benchmark.py {args.command}` compares the LEGACY pairwise engines with each other and with the pairwise reference matcher (app/offline.py). The default engine is now the "
                  f"kernel, which is order-sensitive by design and is not expected to equal that reference.\nRun it as:  MDM_ENGINE=legacy python3 benchmark.py {args.command} ...\n"
                  f"(The kernel's own checks are tests/test_kernel_engine.py and experiments/hard_cases.py.)", file=sys.stderr)
            return 2

    if args.command == "engines":
        from app import benchmark_person
        try:
            report = benchmark_person.engines_trial(args.size, args.workdir, seed=args.seed, budget=args.budget, chunk=args.chunk, log=lambda m: print("  " + m, flush=True))
        except benchmark_person.EnginesPaused as e:
            print(f"\nPAUSED: {e}. Run the same command again to continue.")
            return 3
        print()
        print(benchmark_person.format_engines_trial(report))
        if args.out:
            benchmark.save(report, args.out)
            print(f"\nsaved -> {args.out}")
        return 0 if all(report["identical"].values()) else 1
    if args.command == "delta":
        from app import benchmark_person
        try:
            report = benchmark_person.delta_trial(args.base, args.feed, args.workdir, piece=args.piece, delta_piece=args.delta_piece, budget=args.budget, workers=args.workers, log=lambda m: print("  " + m, flush=True))
        except benchmark_person.DeltaPaused as e:
            print(f"\nPAUSED: {e}. Run the same command again to continue.")
            return 3
        print()
        print(benchmark_person.format_delta_trial(report))
        if args.out:
            benchmark.save(report, args.out)
            print(f"\nsaved -> {args.out}")
        return 0
    if args.command == "bulk":
        from app import benchmark_person
        from app.bulk import BulkPaused
        try:
            report = benchmark_person.bulk_trial(args.size, args.workdir, seed=args.seed, workers=args.workers, chunk=args.chunk, budget=args.budget,
                                                 write=args.write, variant=args.variant, log=lambda m: print("  " + m, flush=True))
        except BulkPaused as e:
            print(f"\nPAUSED: {e}. Run the same command again to continue.")
            return 3
        before = None
        if args.before:
            for sz in benchmark.load(args.before)["sizes"]:
                if sz["records"] == args.size and args.variant in sz["variants"]:
                    v = sz["variants"][args.variant]
                    before = {"pairs_per_record": v["pairs_per_record"], "blocking_recall": v["blocking_recall"], "sample": sz["sample"], "dups": sz["duplicates_in_sample"]}
        print()
        print(benchmark_person.format_bulk_trial(report, before))
        if args.out:
            benchmark.save(report, args.out)
            print(f"\nsaved -> {args.out}")
        return 0
    if args.command == "equivalence":
        from app import offline
        prof = benchmark.get_profile(args.profile, args.variant)
        report = offline.check_equivalence([r["raw"] for r in prof.generate(args.seed, args.scale)], prof.domain, orders=args.orders, seed=args.seed)
        print(offline.format_equivalence(report))
        return 0 if report["ok"] in (True, None) else 1
    if args.command == "scale":
        from app import benchmark_person
        if args.budget is not None and not args.checkpoint:
            print("Error: --budget needs --checkpoint, or the progress would be lost", file=sys.stderr)
            return 2
        base = benchmark.load(args.extend) if args.extend else None
        args.out = args.out or args.extend
        try:
            report = _scale(benchmark_person, args, base)
        except benchmark_person.MeasurementPaused as e:
            print(f"\nPAUSED: {e}. Progress is saved in {args.checkpoint}; run the same command again to continue.")
            return 3
        print()
        print(benchmark_person.format_scale(report))
        if args.out:
            benchmark.save(report, args.out)
            print(f"\nsaved -> {args.out}")
        return 0
    if args.command == "compare":
        print(benchmark.compare(benchmark.load(args.before), benchmark.load(args.after)))
        return 0
    try:
        load_configured_domains(args.domains_dir)
    except DomainConfigError as e:
        print(e, file=sys.stderr)
        return 2
    agg = benchmark.run(args.seeds, args.scale, args.label, args.profile, args.variant)
    print(benchmark.format_report(agg))
    if args.out:
        benchmark.save(agg, args.out)
        print(f"\nsaved -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
