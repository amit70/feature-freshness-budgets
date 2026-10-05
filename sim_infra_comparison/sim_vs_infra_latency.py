"""
sim_vs_infra_latency.py

Tests the hypothesis that the ~0.3s mean age-excess measured in real infra
is what makes real-infra drop rates exceed the pure simulation's.

Method: run the pure sim at each multiplier TWICE --
  (a) extra_scheduling_delay = 0.0  (the original zero-latency model)
  (b) extra_scheduling_delay = 0.3  (nominal decision_window, but the
      served age carries the measured real-infra latency)
-- and compare per-feature drop rates to the real-infra numbers.

If (b)'s drop rates rise to meet the real-infra table, the latency
omission is demonstrated, not just plausible.

Per-feature drops aren't in run()'s row schema (only the count), so this
script re-implements the tiny scoring loop directly against the same
sim_core + arms_multi primitives, reusing default_feature_specs() so the
scenario is identical to run().
"""

import pandas as pd

from run_experiment_multi import default_feature_specs, SCHEDULING_DELAY, HORIZON, REQUEST_INTERVAL
from sim_core import EventGenerator, OracleFeatureStore, LaggedFeatureStore
from arms_multi import arm_t2_multi

MULTIPLIERS = [0.45, 0.50, 0.55, 0.70]
SEEDS = [1, 2, 3, 4, 5]
EXTRA_DELAYS = {"sim_L=0.0": 0.0, "sim_L=0.3": 0.3}

# Real-infra drop rates from analyze_infra_runs.py (for side-by-side)
INFRA = {
    (0.45, "geo_velocity_10min"): 1.000, (0.45, "merchant_diversity_1hr"): 1.000,
    (0.45, "velocity_1hr"): 0.998, (0.45, "velocity_1min"): 0.998, (0.45, "velocity_5min"): 0.998,
    (0.50, "geo_velocity_10min"): 1.000, (0.50, "merchant_diversity_1hr"): 0.982,
    (0.50, "velocity_1hr"): 0.970, (0.50, "velocity_1min"): 0.947, (0.50, "velocity_5min"): 0.953,
    (0.55, "geo_velocity_10min"): 1.000, (0.55, "merchant_diversity_1hr"): 0.895,
    (0.55, "velocity_1hr"): 0.887, (0.55, "velocity_1min"): 0.865, (0.55, "velocity_5min"): 0.873,
    (0.70, "geo_velocity_10min"): 0.733, (0.70, "merchant_diversity_1hr"): 0.592,
    (0.70, "velocity_1hr"): 0.593, (0.70, "velocity_1min"): 0.598, (0.70, "velocity_5min"): 0.612,
}


def run_perfeature(specs, seed, multiplier, extra_delay):
    """Minimal scoring loop that records per-feature drops (T2 only)."""
    burst_start = HORIZON * 0.5
    oracles, stores = {}, {}
    for i, spec in enumerate(specs):
        gen = EventGenerator(entity_id="E1", rate_per_sec=spec.rate_per_sec,
                             seed=seed * 1000 + i, burst_starts=[burst_start],
                             burst_count=spec.burst_count, burst_span=2.0)
        events = gen.generate(HORIZON)
        oracles[spec.name] = OracleFeatureStore(events, window=spec.window)
        stores[spec.name] = LaggedFeatureStore(events, window=spec.window,
                                               ingestion_lag=spec.ingestion_lag,
                                               aggregation_window=spec.aggregation_window)

    effective_delay = SCHEDULING_DELAY + extra_delay
    # decision_window uses NOMINAL delay (operator doesn't know future latency)
    decision_windows = {
        s.name: multiplier * (s.ingestion_lag + s.aggregation_window + SCHEDULING_DELAY)
        for s in specs
    }

    drops = {s.name: 0 for s in specs}
    n = 0
    t = 0.0
    while t < HORIZON:
        outcome = arm_t2_multi(stores, t, effective_delay, decision_windows)
        for name, dropped in outcome.dropped.items():
            if dropped:
                drops[name] += 1
        n += 1
        t += REQUEST_INTERVAL
    return {name: drops[name] / n for name in drops}


def main():
    specs = default_feature_specs()
    rows = []
    for mult in MULTIPLIERS:
        for label, extra in EXTRA_DELAYS.items():
            # average drop rate per feature across seeds
            acc = {s.name: [] for s in specs}
            for seed in SEEDS:
                dr = run_perfeature(specs, seed, mult, extra)
                for name, v in dr.items():
                    acc[name].append(v)
            for name in acc:
                rows.append({
                    "multiplier": mult, "feature": name, "variant": label,
                    "drop_rate": round(sum(acc[name]) / len(acc[name]), 3),
                })

    df = pd.DataFrame(rows)
    # attach infra column
    df["infra_drop_rate"] = df.apply(lambda r: INFRA.get((r.multiplier, r.feature)), axis=1)

    print("=== sim (L=0.0) vs sim (L=0.3) vs real infra, per-feature drop rate ===\n")
    for mult in MULTIPLIERS:
        print(f"--- multiplier = {mult} ---")
        sub = df[df.multiplier == mult]
        piv = sub.pivot(index="feature", columns="variant", values="drop_rate")
        piv["infra"] = sub.groupby("feature").infra_drop_rate.first()
        print(piv.to_string())
        print()

    df.to_csv("sim_vs_infra_latency.csv", index=False)

    # headline: does L=0.3 move sim toward infra? mean |sim - infra| for each variant
    print("=== mean absolute gap to real infra (lower = closer) ===")
    for label in EXTRA_DELAYS:
        sub = df[df.variant == label].dropna(subset=["infra_drop_rate"])
        gap = (sub.drop_rate - sub.infra_drop_rate).abs().mean()
        print(f"  {label}: mean |sim - infra| = {gap:.3f}")


if __name__ == "__main__":
    main()
