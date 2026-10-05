"""
sim_vs_infra_pushmodel.py

Decisive part-3 test. The latency models (constant and stochastic) could
not reproduce velocity_1min @ m=0.70 (sim ~0.0-0.09 vs infra 0.598). The
suspected true cause is architectural, not latency:

  sim_core.LaggedFeatureStore.read() recomputes LAZILY AT READ TIME
  (_recompute_if_due runs inside read), so the served value is always as
  fresh as the read moment allows -- a PULL model.

  Real infra has the consumer and scorer as SEPARATE processes. The
  consumer recomputes on its own grid (poll ticks) and writes Redis; the
  scorer reads whatever is currently there -- a PUSH model. The read is
  decoupled from recompute, so the served value can be up to one full
  aggregation_window stale regardless of when you read.

Predicted consequence for a read at time t: age ranges from
ingestion_lag (just after a grid recompute) to ingestion_lag +
aggregation_window (just before the next one). For velocity_1min
(lag=1.0, agg=1.0), age in [1.0, 2.0]; decision_window@0.70 = 1.435, so
it's over budget whenever (t mod 1.0) > 0.435 -- about 56% of the time,
close to infra's 0.598. The pull model never sees the upper half of that
range, which is why it gave 0.0.

This script re-runs T2 with a PUSH store and compares to infra. If
velocity_1min @ m=0.70 jumps to ~0.56-0.60, the residual is explained by
read/recompute decoupling, and the sim-to-real story is complete.
"""

import bisect
import pandas as pd

from run_experiment_multi import default_feature_specs, SCHEDULING_DELAY, HORIZON, REQUEST_INTERVAL
from sim_core import EventGenerator
from arms_multi import arm_t2_multi, DEFAULT_SAFE_VALUE

MULTIPLIERS = [0.45, 0.50, 0.55, 0.70]
SEEDS = list(range(1, 21))  # 20 seeds -> 20 independent phase draws per feature, for a stable mean

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


class PushLaggedFeatureStore:
    """
    Recompute happens on a fixed grid (every aggregation_window), decoupled
    from reads. read() returns the most recent grid computation at or
    before read_time -- the real push/Redis behavior. Interface-compatible
    with arm_t2_multi (read, mark_trusted, last_trusted_value,
    trusted_history).
    """

    def __init__(self, events, window, ingestion_lag, aggregation_window, phase=0.0):
        self.ingestion_lag = ingestion_lag
        self.aggregation_window = aggregation_window
        self.last_trusted_value = None
        self.trusted_history = []

        self._times, self._watermarks, self._values = [], [], []
        # phase offsets the recompute grid relative to the request clock, so
        # reads sample the WHOLE recompute cycle instead of one fixed point.
        # In real infra the consumer and scorer are independent processes
        # with unsynchronized clocks, so this offset exists and is unknown --
        # phase-locking (offset 0) is the artifact that made the pull sim
        # read velocity_1min at a constant minimal age.
        t = phase
        while t <= HORIZON + aggregation_window:
            watermark = t - ingestion_lag
            lo = watermark - window
            value = sum(1 for e in events if lo <= e.ts <= watermark)
            self._times.append(t)
            self._watermarks.append(watermark)
            self._values.append(value)
            t += aggregation_window

    def mark_trusted(self, value, t=None):
        self.last_trusted_value = value
        if t is not None:
            self.trusted_history.append((t, value))
            if len(self.trusted_history) > 2:
                self.trusted_history.pop(0)

    def read(self, t, scheduling_delay):
        read_time = t + scheduling_delay
        idx = bisect.bisect_right(self._times, read_time) - 1
        if idx < 0:
            return 0, max(read_time, 0.0), None
        watermark = self._watermarks[idx]
        value = self._values[idx]
        return value, max(read_time - watermark, 0.0), watermark


def run_push(specs, seed, multiplier):
    import random
    phase_rng = random.Random(seed * 104729)
    burst_start = HORIZON * 0.5
    stores = {}
    for i, spec in enumerate(specs):
        gen = EventGenerator(entity_id="E1", rate_per_sec=spec.rate_per_sec,
                             seed=seed * 1000 + i, burst_starts=[burst_start],
                             burst_count=spec.burst_count, burst_span=2.0)
        events = gen.generate(HORIZON)
        # random phase offset in [0, aggregation_window): models the unknown,
        # unsynchronized timing between the consumer's recompute grid and the
        # scorer's request clock in real infra.
        phase = phase_rng.uniform(0, spec.aggregation_window)
        stores[spec.name] = PushLaggedFeatureStore(
            events, window=spec.window, ingestion_lag=spec.ingestion_lag,
            aggregation_window=spec.aggregation_window, phase=phase)

    decision_windows = {
        s.name: multiplier * (s.ingestion_lag + s.aggregation_window + SCHEDULING_DELAY)
        for s in specs
    }
    drops = {s.name: 0 for s in specs}
    n = 0
    t = 0.0
    while t < HORIZON:
        outcome = arm_t2_multi(stores, t, SCHEDULING_DELAY, decision_windows)
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
        acc = {s.name: [] for s in specs}
        for seed in SEEDS:
            dr = run_push(specs, seed, mult)
            for name, v in dr.items():
                acc[name].append(v)
        for name in acc:
            rows.append({"multiplier": mult, "feature": name,
                         "sim_push_drop_rate": round(sum(acc[name]) / len(acc[name]), 3),
                         "infra_drop_rate": INFRA.get((mult, name))})

    df = pd.DataFrame(rows)
    for mult in MULTIPLIERS:
        print(f"--- multiplier = {mult} ---")
        sub = df[df.multiplier == mult][["feature", "sim_push_drop_rate", "infra_drop_rate"]]
        print(sub.to_string(index=False))
        print()

    df.to_csv("sim_vs_infra_pushmodel.csv", index=False)
    gap = (df.sim_push_drop_rate - df.infra_drop_rate).abs().mean()
    print(f"=== mean |sim_push - infra| = {gap:.3f} ===")
    print("(compare: sim_const_0.3 = 0.063, sim_stoch = 0.070 from the latency tests)")
    print()
    v = df[(df.multiplier == 0.70) & (df.feature == "velocity_1min")].sim_push_drop_rate.iloc[0]
    print(f"spotlight velocity_1min @ m=0.70: sim_push={v:.3f}  (infra 0.598; "
          f"pull-model sim was 0.000, stochastic 0.086)")


if __name__ == "__main__":
    main()
