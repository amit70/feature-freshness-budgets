"""
sim_vs_infra_variance.py

Part-3 test: the constant +0.3s offset closed the tight-budget gaps but
left a residual at looser multipliers (e.g. velocity_1min @ m=0.70 stayed
at 0.0 in sim vs 0.598 in infra). Hypothesis: real-infra latency is a
DISTRIBUTION (mean ~0.3s, heavy tail up to several seconds), and it's the
tail -- not the mean -- that drops features with >0.3s budget headroom.

This draws a per-request latency from a mixture calibrated to the observed
excess stats (median 0.25, mean 0.32, max 8.05):
    base  ~ Exponential(mean 0.13)                 every request
    stall ~ Exponential(mean 3.5) with prob 0.05   occasional spike
    => mean ~ 0.13 + 0.05*3.5 = 0.305, tail to several seconds.

It's a plausible model calibrated to the summary stats, NOT a directly
measured per-read trace -- stated as such. Compares three variants per
feature: constant L=0.3, stochastic L (same mean, with tail), real infra.
"""

import random
import pandas as pd

from run_experiment_multi import default_feature_specs, SCHEDULING_DELAY, HORIZON, REQUEST_INTERVAL
from sim_core import EventGenerator, OracleFeatureStore, LaggedFeatureStore
from arms_multi import arm_t2_multi

MULTIPLIERS = [0.45, 0.50, 0.55, 0.70]
SEEDS = [1, 2, 3, 4, 5]

# stochastic latency parameters (calibrated to observed excess stats)
BASE_MEAN = 0.13
STALL_PROB = 0.05
STALL_MEAN = 3.5

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


def draw_latency(rng):
    L = rng.expovariate(1.0 / BASE_MEAN)
    if rng.random() < STALL_PROB:
        L += rng.expovariate(1.0 / STALL_MEAN)
    return L


def build(specs, seed):
    burst_start = HORIZON * 0.5
    stores = {}
    for i, spec in enumerate(specs):
        gen = EventGenerator(entity_id="E1", rate_per_sec=spec.rate_per_sec,
                             seed=seed * 1000 + i, burst_starts=[burst_start],
                             burst_count=spec.burst_count, burst_span=2.0)
        events = gen.generate(HORIZON)
        stores[spec.name] = LaggedFeatureStore(events, window=spec.window,
                                               ingestion_lag=spec.ingestion_lag,
                                               aggregation_window=spec.aggregation_window)
    return stores


def run_variant(specs, seed, multiplier, mode):
    """mode: 'const' (L=0.3 fixed) or 'stoch' (drawn per request)."""
    stores = build(specs, seed)
    decision_windows = {
        s.name: multiplier * (s.ingestion_lag + s.aggregation_window + SCHEDULING_DELAY)
        for s in specs
    }
    lat_rng = random.Random(seed * 7919)
    drops = {s.name: 0 for s in specs}
    n = 0
    t = 0.0
    while t < HORIZON:
        if mode == "const":
            delay = SCHEDULING_DELAY + 0.3
        else:
            delay = SCHEDULING_DELAY + draw_latency(lat_rng)
        outcome = arm_t2_multi(stores, t, delay, decision_windows)
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
        for mode, label in [("const", "sim_const_0.3"), ("stoch", "sim_stoch")]:
            acc = {s.name: [] for s in specs}
            for seed in SEEDS:
                dr = run_variant(specs, seed, mult, mode)
                for name, v in dr.items():
                    acc[name].append(v)
            for name in acc:
                rows.append({"multiplier": mult, "feature": name, "variant": label,
                             "drop_rate": round(sum(acc[name]) / len(acc[name]), 3)})

    df = pd.DataFrame(rows)
    df["infra"] = df.apply(lambda r: INFRA.get((r.multiplier, r.feature)), axis=1)

    for mult in MULTIPLIERS:
        print(f"--- multiplier = {mult} ---")
        sub = df[df.multiplier == mult]
        piv = sub.pivot(index="feature", columns="variant", values="drop_rate")
        piv["infra"] = sub.groupby("feature").infra.first()
        print(piv.to_string())
        print()

    df.to_csv("sim_vs_infra_variance.csv", index=False)
    print("=== mean absolute gap to real infra (lower = closer) ===")
    for label in ["sim_const_0.3", "sim_stoch"]:
        sub = df[df.variant == label].dropna(subset=["infra"])
        gap = (sub.drop_rate - sub.infra).abs().mean()
        print(f"  {label}: mean |sim - infra| = {gap:.3f}")

    print("\n=== spotlight: velocity_1min @ m=0.70 (the residual the constant couldn't move) ===")
    for label in ["sim_const_0.3", "sim_stoch"]:
        v = df[(df.multiplier == 0.70) & (df.feature == "velocity_1min") & (df.variant == label)].drop_rate.iloc[0]
        print(f"  {label}: {v:.3f}   (infra: 0.598)")


if __name__ == "__main__":
    main()
