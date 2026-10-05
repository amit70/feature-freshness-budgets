"""
Fine-grained sweep across the predicted transition band (0.40-0.65) to
resolve the error-recovery curve against the two closed-form thresholds:
  min_f(m*_f) ~= 0.501  (merchant_diversity_1hr) -- where OR-decision
                 accuracy should start recovering
  max_f(m*_f) = 0.6008  (geo_velocity_10min)      -- where full
                 completeness (mean_n_dropped -> 0) should be reached
"""
import csv
import sys
from run_experiment_multi import default_feature_specs, run
from arms_multi import ARMS_MULTI

MULTIPLIERS = [round(0.40 + 0.01 * i, 2) for i in range(26)]  # 0.40..0.65
SEEDS = [1, 2, 3, 4, 5]
OUT_PATH = "fine_matrix_multi_results.csv"

def main():
    specs = default_feature_specs()
    all_rows = []
    total = len(MULTIPLIERS) * len(SEEDS) * len(ARMS_MULTI)
    done = 0
    for m in MULTIPLIERS:
        for seed in SEEDS:
            for arm_name in ARMS_MULTI:
                rows = run(specs, seed, m, arm_name)
                all_rows.extend(rows)
                done += 1
                if done % 50 == 0:
                    print(f"{done}/{total} cells complete", file=sys.stderr)
    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {OUT_PATH}")

if __name__ == "__main__":
    main()
