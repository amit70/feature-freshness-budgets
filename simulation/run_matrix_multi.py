import csv
import sys
from run_experiment_multi import default_feature_specs, run
from arms_multi import ARMS_MULTI

MULTIPLIERS = [0.3, 0.5, 0.7, 0.9, 1.2, 1.5, 2.0, 3.0]
SEEDS = [1, 2, 3, 4, 5]

OUT_PATH = "matrix_multi_results.csv"


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
                if done % 20 == 0:
                    print(f"{done}/{total} cells complete", file=sys.stderr)

    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
