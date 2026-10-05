"""
Single-scenario experiment: one entity's transaction stream, one fraud-style
velocity decision ("flag if txn_count_5min >= THRESHOLD"), evaluated across
all three arms against the same event stream and same request schedule.

Every read asserts Proposition 1 so a violation is a hard failure, not a
silent possibility:
    age <= ingestion_lag + aggregation_window + scheduling_delay
"""

import argparse
import csv
import random
import sys
from dataclasses import asdict, dataclass

from sim_core import EventGenerator, OracleFeatureStore, LaggedFeatureStore
from arms import ARMS

FRAUD_THRESHOLD = 5          # flag if txn_count_5min >= this
WINDOW = 300.0                # 5-minute sliding window, seconds
SCHEDULING_DELAY = 0.05       # fixed request-serving overhead, seconds


@dataclass
class RunConfig:
    seed: int
    horizon: float = 600.0            # 10 min simulated
    background_rate: float = 0.05     # txns/sec baseline
    burst_count: int = 8              # extra txns injected per burst
    ingestion_lag: float = 5.0
    aggregation_window: float = 5.0
    decision_window: float = 15.0
    request_interval: float = 1.0     # decision requests issued this often


def run(cfg: RunConfig, arm_name: str) -> list:
    rng = random.Random(cfg.seed)
    # inject one burst roughly mid-horizon to create a genuine staleness test
    burst_start = cfg.horizon * 0.5
    gen = EventGenerator(
        entity_id="E1", rate_per_sec=cfg.background_rate, seed=cfg.seed,
        burst_starts=[burst_start], burst_count=cfg.burst_count, burst_span=2.0,
    )
    events = gen.generate(cfg.horizon)

    oracle = OracleFeatureStore(events, window=WINDOW)
    lagged = LaggedFeatureStore(
        events, window=WINDOW,
        ingestion_lag=cfg.ingestion_lag,
        aggregation_window=cfg.aggregation_window,
    )
    arm_fn = ARMS[arm_name]

    rows = []
    t = 0.0
    while t < cfg.horizon:
        true_value = oracle.query(t)
        true_decision = true_value >= FRAUD_THRESHOLD

        if arm_name == "T1":
            outcome = arm_fn(lagged, t, SCHEDULING_DELAY, cfg.decision_window)
        else:
            outcome = arm_fn(lagged, t, SCHEDULING_DELAY, cfg.decision_window)

        bound = cfg.ingestion_lag + cfg.aggregation_window + SCHEDULING_DELAY
        assert outcome.age <= bound + 1e-6, (
            f"Proposition 1 VIOLATED: age={outcome.age:.3f} > bound={bound:.3f} "
            f"(lag={cfg.ingestion_lag}, agg={cfg.aggregation_window}, sched={SCHEDULING_DELAY})"
        )

        served_decision = (not outcome.dropped) and (outcome.served_value >= FRAUD_THRESHOLD)
        error = served_decision != true_decision
        false_negative = true_decision and not served_decision
        false_positive = served_decision and not true_decision

        rows.append({
            "t": round(t, 3),
            "arm": arm_name,
            "seed": cfg.seed,
            "ingestion_lag": cfg.ingestion_lag,
            "aggregation_window": cfg.aggregation_window,
            "decision_window": cfg.decision_window,
            "background_rate": cfg.background_rate,
            "burst_count": cfg.burst_count,
            "true_value": true_value,
            "served_value": outcome.served_value,
            "age": round(outcome.age, 4),
            "budget": round(outcome.budget, 4),
            "dropped": outcome.dropped,
            "timed_out": outcome.timed_out,
            "added_latency": round(outcome.added_latency, 4),
            "error": error,
            "false_negative": false_negative,
            "false_positive": false_positive,
        })
        t += cfg.request_interval

    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--ingestion_lag", type=float, default=5.0)
    p.add_argument("--aggregation_window", type=float, default=5.0)
    p.add_argument("--decision_window", type=float, default=15.0)
    p.add_argument("--out", type=str, default="run_single.csv")
    args = p.parse_args()

    cfg = RunConfig(
        seed=args.seed,
        ingestion_lag=args.ingestion_lag,
        aggregation_window=args.aggregation_window,
        decision_window=args.decision_window,
    )

    all_rows = []
    for arm_name in ARMS:
        rows = run(cfg, arm_name)
        all_rows.extend(rows)
        n_err = sum(r["error"] for r in rows)
        n_fn = sum(r["false_negative"] for r in rows)
        n_drop = sum(r["dropped"] for r in rows)
        print(f"[{arm_name}] requests={len(rows)} errors={n_err} "
              f"false_negatives={n_fn} dropped={n_drop}")

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
