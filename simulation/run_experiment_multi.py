"""
Multi-feature experiment: K features per request, heterogeneous lag and
cadence, decision = OR across features (any feature over its threshold
flags the request). Same Proposition 1 assertion, now checked per feature
per read.
"""

import argparse
import csv
import random
import sys
from dataclasses import dataclass, field
from typing import List

from sim_core import EventGenerator, OracleFeatureStore, LaggedFeatureStore, FeatureSpec
from arms_multi import ARMS_MULTI

SCHEDULING_DELAY = 0.05
HORIZON = 600.0
REQUEST_INTERVAL = 1.0


def default_feature_specs() -> List[FeatureSpec]:
    """
    5 heterogeneous features, deliberately spanning fast/cheap to
    slow/expensive to compute — this spread is what makes T1's
    block-on-slowest cost show up.
    """
    return [
        FeatureSpec("velocity_1min", rate_per_sec=0.03, threshold=2, window=60.0,
                    ingestion_lag=1.0, aggregation_window=1.0, burst_count=3),
        FeatureSpec("velocity_5min", rate_per_sec=0.05, threshold=5, window=300.0,
                    ingestion_lag=2.0, aggregation_window=2.0, burst_count=8),
        FeatureSpec("velocity_1hr", rate_per_sec=0.08, threshold=15, window=3600.0,
                    ingestion_lag=5.0, aggregation_window=5.0, burst_count=10),
        FeatureSpec("merchant_diversity_1hr", rate_per_sec=0.04, threshold=8, window=3600.0,
                    ingestion_lag=10.0, aggregation_window=10.0, burst_count=6),
        FeatureSpec("geo_velocity_10min", rate_per_sec=0.02, threshold=3, window=600.0,
                    ingestion_lag=15.0, aggregation_window=10.0, burst_count=4),
    ]


def build_stores(specs: List[FeatureSpec], seed: int, burst_start: float):
    oracles, stores, decision_windows = {}, {}, {}
    for i, spec in enumerate(specs):
        gen = EventGenerator(
            entity_id="E1", rate_per_sec=spec.rate_per_sec, seed=seed * 1000 + i,
            burst_starts=[burst_start], burst_count=spec.burst_count, burst_span=2.0,
        )
        events = gen.generate(HORIZON)
        oracles[spec.name] = (OracleFeatureStore(events, window=spec.window), spec.threshold)
        stores[spec.name] = LaggedFeatureStore(
            events, window=spec.window,
            ingestion_lag=spec.ingestion_lag,
            aggregation_window=spec.aggregation_window,
        )
        decision_windows[spec.name] = None  # filled in by caller (swept param)
    return oracles, stores


def run(specs: List[FeatureSpec], seed: int, decision_window_multiplier: float,
        arm_name: str, t1_timeout: float = 10.0, t1_poll_interval: float = 0.5,
        t2_fallback_strategy: str = "last_trusted", extra_scheduling_delay: float = 0.0) -> list:
    """
    decision_window_multiplier scales each feature's OWN bound
    (ingestion_lag + aggregation_window + SCHEDULING_DELAY) to derive its
    decision_window — e.g. 1.0 means "exactly as tight as the bound",
    2.0 means "twice as forgiving". This lets one sweep parameter tighten
    or loosen the budget uniformly across heterogeneous features.
    """
    burst_start = HORIZON * 0.5
    oracles, stores = build_stores(specs, seed, burst_start)

    effective_scheduling_delay = SCHEDULING_DELAY + extra_scheduling_delay

    # decision_windows are the OPERATOR'S ORIGINAL SLA, set using the
    # nominal (unloaded) scheduling delay -- they don't know about future
    # load when they set this.
    decision_windows = {}
    for spec in specs:
        nominal_bound = spec.ingestion_lag + spec.aggregation_window + SCHEDULING_DELAY
        decision_windows[spec.name] = nominal_bound * decision_window_multiplier

    arm_fn = ARMS_MULTI[arm_name]
    rows = []
    t = 0.0
    while t < HORIZON:
        true_decision = False
        for name, (oracle, threshold) in oracles.items():
            if oracle.query(t) >= threshold:
                true_decision = True

        if arm_name == "T1":
            outcome = arm_fn(stores, t, effective_scheduling_delay, decision_windows,
                              poll_interval=t1_poll_interval, timeout=t1_timeout)
        elif arm_name == "T2":
            outcome = arm_fn(stores, t, effective_scheduling_delay, decision_windows,
                              fallback_strategy=t2_fallback_strategy)
        else:
            outcome = arm_fn(stores, t, effective_scheduling_delay, decision_windows)

        # Proposition 1, checked against the TRUE current bound (which
        # includes whatever extra delay load is actually imposing) -- this
        # must still hold; it's a physical fact, not a promise.
        for spec in specs:
            actual_bound = spec.ingestion_lag + spec.aggregation_window + effective_scheduling_delay
            age = outcome.ages[spec.name]
            assert age <= actual_bound + 1e-6, (
                f"Proposition 1 VIOLATED on {spec.name}: age={age:.3f} > bound={actual_bound:.3f}"
            )

        served_decision = False
        for spec in specs:
            oracle, threshold = oracles[spec.name]
            if outcome.served_values[spec.name] >= threshold:
                served_decision = True

        n_dropped = sum(outcome.dropped.values())
        error = served_decision != true_decision

        rows.append({
            "t": round(t, 3),
            "arm": arm_name,
            "seed": seed,
            "decision_window_multiplier": decision_window_multiplier,
            "n_features": len(specs),
            "n_dropped": n_dropped,
            "request_latency": round(outcome.request_latency, 4),
            "timed_out": outcome.timed_out,
            "true_decision": true_decision,
            "served_decision": served_decision,
            "error": error,
            "false_negative": true_decision and not served_decision,
            "false_positive": served_decision and not true_decision,
            "max_age": round(max(outcome.ages.values()), 4),
        })
        t += REQUEST_INTERVAL

    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--decision_window_multiplier", type=float, default=1.0)
    p.add_argument("--out", type=str, default="run_multi_single.csv")
    args = p.parse_args()

    specs = default_feature_specs()
    all_rows = []
    for arm_name in ARMS_MULTI:
        rows = run(specs, args.seed, args.decision_window_multiplier, arm_name)
        all_rows.extend(rows)
        n_err = sum(r["error"] for r in rows)
        n_fn = sum(r["false_negative"] for r in rows)
        mean_lat = sum(r["request_latency"] for r in rows) / len(rows)
        mean_drop = sum(r["n_dropped"] for r in rows) / len(rows)
        print(f"[{arm_name}] requests={len(rows)} errors={n_err} false_negatives={n_fn} "
              f"mean_request_latency={mean_lat:.3f}s mean_features_dropped={mean_drop:.2f}")

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
