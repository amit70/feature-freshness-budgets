"""
Matrix sweep, same pattern as the paper-1 headline runs: independent
variables crossed, multiple seeds per cell for mean +/- std.
"""

import csv
import itertools
import sys

from run_experiment import RunConfig, run
from arms import ARMS

# Sweep ranges. Kept modest for a first pass; widen once the shape looks right.
INGESTION_LAGS = [1.0, 5.0, 10.0]
AGGREGATION_WINDOWS = [2.0, 5.0]
DECISION_WINDOWS = [8.0, 15.0, 30.0]
BURST_COUNTS = [4, 12]           # low vs high burst intensity
SEEDS = [1, 2, 3]

OUT_PATH = "matrix_results.csv"


def main():
    all_rows = []
    combos = list(itertools.product(
        INGESTION_LAGS, AGGREGATION_WINDOWS, DECISION_WINDOWS, BURST_COUNTS, SEEDS
    ))
    total = len(combos) * len(ARMS)
    done = 0

    for lag, agg, dwin, burst, seed in combos:
        cfg = RunConfig(
            seed=seed,
            ingestion_lag=lag,
            aggregation_window=agg,
            decision_window=dwin,
            burst_count=burst,
        )
        for arm_name in ARMS:
            rows = run(cfg, arm_name)
            all_rows.extend(rows)
            done += 1
            if done % 20 == 0:
                print(f"{done}/{total} cells complete", file=sys.stderr)

    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows across {len(combos)} cells x {len(ARMS)} arms to {OUT_PATH}")


if __name__ == "__main__":
    main()
