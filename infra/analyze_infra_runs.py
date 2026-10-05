"""
analyze_infra_runs.py

Ingests every valid infra scorer run in a directory and builds the
real-infra threshold table for the paper: for each multiplier, the
decision-level error rate, mean features dropped, and the Proposition-1
violation rate, plus a per-feature drop-rate breakdown.

It pairs each `infra_run_m<M>.csv` (decision-level) with its
`infra_run_m<M>_perfeature.csv` (per-feature detail). Runs missing the
per-feature file are still summarized at the decision level.

INVALID-RUN GUARD: any run whose per-feature file contains impossible
ages (negative, or a large fraction over the horizon) is flagged and
EXCLUDED from the summary -- this catches the clock-desync bug so a void
run can't silently pollute the paper table.

Usage:
    python3 analyze_infra_runs.py                 # scans current dir
    python3 analyze_infra_runs.py --dir ./runs    # scans ./runs
    python3 analyze_infra_runs.py --horizon 600   # for the age-sanity check

Outputs:
    - prints the threshold table and per-feature drop-rate table
    - writes infra_threshold_summary.csv (one row per valid multiplier)
    - writes infra_perfeature_summary.csv (one row per multiplier x feature)
"""

import argparse
import glob
import os
import re
import sys

import pandas as pd

# Predicted per-feature permanent-drop thresholds m*_f (from Proposition 2)
MSTAR = {
    "velocity_1min": 0.5122,
    "velocity_5min": 0.5062,
    "velocity_1hr": 0.5025,
    "merchant_diversity_1hr": 0.5012,
    "geo_velocity_10min": 0.6008,
}
MSTAR_MIN = min(MSTAR.values())  # 0.5012 -- system-level collapse point
MSTAR_MAX = max(MSTAR.values())  # 0.6008 -- full-completeness point


def find_runs(directory):
    """Return {multiplier: {'decision': path, 'perfeature': path|None}}."""
    runs = {}
    for path in glob.glob(os.path.join(directory, "infra_run_m*.csv")):
        base = os.path.basename(path)
        if base.endswith("_perfeature.csv"):
            continue
        m = re.match(r"infra_run_m([0-9.]+)\.csv$", base)
        if not m:
            continue
        mult = float(m.group(1))
        pf = path.replace(".csv", "_perfeature.csv")
        runs[mult] = {"decision": path, "perfeature": pf if os.path.exists(pf) else None}
    return dict(sorted(runs.items()))


def validate_ages(pf_df, horizon):
    """Return (is_valid, reason). Catches the clock-desync signature."""
    if "age" not in pf_df.columns:
        return True, ""  # can't check; assume ok
    neg_frac = (pf_df.age < -1.0).mean()
    huge_frac = (pf_df.age > horizon).mean()
    if neg_frac > 0.01:
        return False, f"{neg_frac:.1%} of ages negative (clock desync)"
    if huge_frac > 0.05:
        return False, f"{huge_frac:.1%} of ages exceed horizon (clock desync)"
    return True, ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".", help="directory containing infra_run_m*.csv files")
    ap.add_argument("--horizon", type=float, default=600.0)
    args = ap.parse_args()

    runs = find_runs(args.dir)
    if not runs:
        print(f"No infra_run_m*.csv files found in {args.dir}", file=sys.stderr)
        sys.exit(1)

    threshold_rows = []
    perfeature_rows = []

    for mult, files in runs.items():
        dec = pd.read_csv(files["decision"])
        row = {
            "multiplier": mult,
            "requests": len(dec),
            "error_rate": round(dec.error.mean(), 4),
            "mean_n_dropped": round(dec.n_dropped.mean(), 3),
            "max_n_dropped": int(dec.n_dropped.max()),
        }

        if files["perfeature"]:
            pf = pd.read_csv(files["perfeature"])
            valid, reason = validate_ages(pf, args.horizon)
            if not valid:
                row["status"] = f"INVALID: {reason}"
                row["prop1_viol_rate"] = None
                threshold_rows.append(row)
                print(f"  [skip] m={mult}: {reason} -- excluded from summary")
                continue
            row["status"] = "ok"
            row["prop1_viol_rate"] = round(pf.over_prop1_bound.mean(), 4) if "over_prop1_bound" in pf else None
            row["mean_age_excess"] = None
            if "over_prop1_bound" in pf and pf.over_prop1_bound.any():
                v = pf[pf.over_prop1_bound]
                row["mean_age_excess"] = round((v.age - v.prop1_bound).mean(), 3)

            for feat in MSTAR:
                sub = pf[pf.feature == feat]
                if len(sub) == 0:
                    continue
                predicted_drop = mult < MSTAR[feat]
                perfeature_rows.append({
                    "multiplier": mult,
                    "feature": feat,
                    "m_star": MSTAR[feat],
                    "drop_rate": round(sub.dropped.mean(), 3),
                    "predicted_permanent_drop": predicted_drop,
                    "prop1_viol_rate": round(sub.over_prop1_bound.mean(), 3) if "over_prop1_bound" in sub else None,
                })
        else:
            row["status"] = "no per-feature file"
            row["prop1_viol_rate"] = None

        threshold_rows.append(row)

    tdf = pd.DataFrame(threshold_rows).sort_values("multiplier")
    tdf.to_csv("infra_threshold_summary.csv", index=False)
    print("\n=== REAL-INFRA THRESHOLD TABLE ===")
    print(f"(predicted collapse below m={MSTAR_MIN:.4f}; full completeness at m>={MSTAR_MAX:.4f})")
    print(tdf.to_string(index=False))

    if perfeature_rows:
        pdf = pd.DataFrame(perfeature_rows).sort_values(["multiplier", "m_star"])
        pdf.to_csv("infra_perfeature_summary.csv", index=False)
        print("\n=== PER-FEATURE DROP RATES (valid runs only) ===")
        print("drop_rate should approach 1.0 exactly when multiplier < m_star")
        pivot = pdf.pivot(index="feature", columns="multiplier", values="drop_rate")
        print(pivot.to_string())

    print("\nWrote infra_threshold_summary.csv and infra_perfeature_summary.csv")


if __name__ == "__main__":
    main()
