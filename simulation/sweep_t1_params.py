"""
T1 pays latency to guarantee freshness. This sweep characterizes that
cost: how much does tightening/loosening the poll_interval and timeout
change mean/p95/p99 request latency and the resulting error rate?

Run at multiplier=0.7 (T1 already low-error there per the main sweep,
1.2% -- interesting question is how cheaply that accuracy can be bought)
and multiplier=0.3 (T1's worst case, 9.0s mean latency with default
params -- can smarter polling reduce that without sacrificing accuracy?).
"""

import csv
import sys
import pandas as pd
from run_experiment_multi import default_feature_specs, run

TIMEOUTS = [1.0, 2.0, 5.0, 10.0, 20.0]
POLL_INTERVALS = [0.1, 0.25, 0.5, 1.0]
MULTIPLIERS = [0.3, 0.7]
SEEDS = [1, 2, 3, 4, 5]

OUT_PATH = "t1_param_sweep_results.csv"


def main():
    specs = default_feature_specs()
    all_rows = []
    combos = [(m, to, pi, s) for m in MULTIPLIERS for to in TIMEOUTS
              for pi in POLL_INTERVALS for s in SEEDS]
    total = len(combos)
    done = 0
    for m, timeout, poll, seed in combos:
        rows = run(specs, seed, m, "T1", t1_timeout=timeout, t1_poll_interval=poll)
        for r in rows:
            r["t1_timeout"] = timeout
            r["t1_poll_interval"] = poll
        all_rows.extend(rows)
        done += 1
        if done % 20 == 0:
            print(f"{done}/{total} cells complete", file=sys.stderr)

    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {OUT_PATH}")

    df = pd.DataFrame(all_rows)
    summary = df.groupby(["decision_window_multiplier", "t1_timeout", "t1_poll_interval"]).agg(
        error_rate=("error", "mean"),
        mean_latency=("request_latency", "mean"),
        p95_latency=("request_latency", lambda x: x.quantile(0.95)),
        p99_latency=("request_latency", lambda x: x.quantile(0.99)),
        timeout_rate=("timed_out", "mean"),
    ).reset_index()
    summary.to_csv("t1_param_sweep_summary.csv", index=False)
    print("\n=== multiplier=0.7 (T1's easy case) ===")
    print(summary[summary.decision_window_multiplier == 0.7].round(4).to_string(index=False))
    print("\n=== multiplier=0.3 (T1's hard case) ===")
    print(summary[summary.decision_window_multiplier == 0.3].round(4).to_string(index=False))


if __name__ == "__main__":
    main()
