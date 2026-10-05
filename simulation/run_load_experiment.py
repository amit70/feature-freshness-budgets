"""
Tests whether load-induced queueing delay -- entirely on its own, with no
change to the operator's configured decision_window -- can push a feature
across its Proposition 2 threshold. This is the "why did my SLA that was
fine yesterday start failing today" scenario: nothing about the freshness
policy changed, only how many entities are sharing the compute pipeline.

Fixed: multiplier=0.7 (safe for all 5 features when unloaded -- see the
fine sweep, error_rate=0.4%, mean_n_dropped=2.4 there is from the normal
per-cycle timing gap, not collapse). Fixed: a 4-worker pool, 0.2s
processing time per recompute (capacity = 20 jobs/sec). Swept: number of
entities sharing that pool.
"""

import csv
import sys
from contention import mean_queue_delay
from run_experiment_multi import default_feature_specs, run

N_WORKERS = 4
PROCESSING_TIME = 0.2  # seconds per recompute job
HORIZON = 600.0
MULTIPLIER = 0.7
SEEDS = [1, 2, 3, 4, 5]
N_ENTITIES_SWEEP = [1, 10, 50, 100, 200, 300, 400, 500]

# Use the feature with the shortest aggregation_window as the load proxy
# (it recomputes most often, so it's the one most exposed to a shared
# pool's queueing under load) -- merchant_diversity_1hr has agg=10s.
PROXY_AGG_WINDOW = 10.0


def main():
    specs = default_feature_specs()

    print(f"{'n_entities':>10} {'mean_queue_delay':>18} {'p95_queue_delay':>16} {'n_jobs':>8}")
    load_rows = []
    for n in N_ENTITIES_SWEEP:
        mean_d, p95_d, n_jobs = mean_queue_delay(
            n_entities=n, aggregation_window=PROXY_AGG_WINDOW, horizon=HORIZON,
            processing_time=PROCESSING_TIME, n_workers=N_WORKERS, seed=1,
        )
        print(f"{n:>10} {mean_d:>18.4f} {p95_d:>16.4f} {n_jobs:>8}")
        load_rows.append({"n_entities": n, "mean_queue_delay": mean_d, "p95_queue_delay": p95_d})

    with open("load_queue_delay.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=load_rows[0].keys())
        w.writeheader()
        w.writerows(load_rows)

    print(f"\nRunning decision experiment at multiplier={MULTIPLIER} under each load level "
          f"(mean_queue_delay injected as extra_scheduling_delay)...")
    all_rows = []
    for lr in load_rows:
        n, extra_delay = lr["n_entities"], lr["mean_queue_delay"]
        for seed in SEEDS:
            for arm_name in ["B0", "T1", "T2"]:
                rows = run(specs, seed, MULTIPLIER, arm_name, extra_scheduling_delay=extra_delay)
                for r in rows:
                    r["n_entities"] = n
                    r["injected_queue_delay"] = extra_delay
                all_rows.extend(rows)

    with open("load_experiment_results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        w.writeheader()
        w.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to load_experiment_results.csv")

    import pandas as pd
    df = pd.DataFrame(all_rows)
    summary = df.groupby(["arm", "n_entities"]).agg(
        error_rate=("error", "mean"),
        mean_n_dropped=("n_dropped", "mean"),
        mean_request_latency=("request_latency", "mean"),
    ).reset_index()
    summary.to_csv("load_experiment_summary.csv", index=False)
    print("\n=== error_rate and mean_n_dropped vs n_entities, by arm ===")
    for arm in ["B0", "T1", "T2"]:
        print(f"\n--- {arm} ---")
        print(summary[summary.arm == arm].drop(columns="arm").round(4).to_string(index=False))


if __name__ == "__main__":
    main()
