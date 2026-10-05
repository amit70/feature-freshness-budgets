"""
Tests the prediction from Proposition 2: the collapse THRESHOLD (which
multiplier a feature becomes permanently droppable at) is fixed by
(lag, aggregation_window, scheduling_delay) alone and cannot be moved by
the fallback strategy. What the fallback CAN change is the error rate
DURING collapse (m < min_f(m*_f) ~= 0.5012), since a smarter guess about
the dropped feature's value can still be right sometimes even though the
feature is never actually served fresh.
"""

import csv
import sys
from run_experiment_multi import default_feature_specs, run

STRATEGIES = ["last_trusted", "decay", "extrapolate"]
MULTIPLIERS = [0.30, 0.40, 0.45, 0.48, 0.50, 0.51, 0.55, 0.60, 0.65, 0.70]
SEEDS = [1, 2, 3, 4, 5]

OUT_PATH = "fallback_strategy_results.csv"


def main():
    specs = default_feature_specs()
    all_rows = []
    combos = [(m, strat, s) for m in MULTIPLIERS for strat in STRATEGIES for s in SEEDS]
    total = len(combos)
    done = 0
    for m, strat, seed in combos:
        rows = run(specs, seed, m, "T2", t2_fallback_strategy=strat)
        for r in rows:
            r["fallback_strategy"] = strat
        all_rows.extend(rows)
        done += 1
        if done % 20 == 0:
            print(f"{done}/{total} cells complete", file=sys.stderr)

    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {OUT_PATH}")

    import pandas as pd
    df = pd.DataFrame(all_rows)
    summary = df.groupby(["fallback_strategy", "decision_window_multiplier"]).agg(
        error_rate=("error", "mean"),
        mean_n_dropped=("n_dropped", "mean"),
    ).reset_index()
    summary.to_csv("fallback_strategy_summary.csv", index=False)

    pivot = summary.pivot(index="decision_window_multiplier", columns="fallback_strategy", values="error_rate")
    print("\n=== error_rate by fallback strategy across the transition band ===")
    print(pivot.round(4).to_string())

    drop_pivot = summary.pivot(index="decision_window_multiplier", columns="fallback_strategy", values="mean_n_dropped")
    print("\n=== mean_n_dropped by fallback strategy (should be IDENTICAL across strategies -- Prop 2 predicts this) ===")
    print(drop_pivot.round(3).to_string())


if __name__ == "__main__":
    main()
