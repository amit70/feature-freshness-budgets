"""
Multi-feature version of the three policy arms.

A decision request now depends on K features, each with its OWN
ingestion_lag / aggregation_window / threshold. This is the setting where
T1 and T2 are actually supposed to differ from each other and from B0:

  - B0: read every feature's current (possibly stale) value, no waiting,
    no budget check.
  - T1: block the ENTIRE request until ALL K features are within their
    own decision_window, or give up at a timeout. The request pays the
    wait for the SLOWEST feature.
  - T2: read each feature independently against its own budget. A feature
    that's over budget is not blocked on — it's replaced by its
    last-trusted (last known in-budget) value and flagged. Fast features
    are never held hostage by a slow one.

Decision rule: flag = OR across features (any feature over its own
threshold triggers a flag) — a simple, defensible fraud-style rule.
"""

from dataclasses import dataclass, field
from typing import Dict, List
from sim_core import LaggedFeatureStore

DEFAULT_SAFE_VALUE = 0  # cold-start fallback only, before any trusted value exists


@dataclass
class MultiReadOutcome:
    served_values: Dict[str, float]
    ages: Dict[str, float]
    budgets: Dict[str, float]
    dropped: Dict[str, bool]
    request_latency: float   # total added wait beyond base scheduling delay
    timed_out: bool = False


def _budget(store: LaggedFeatureStore, name, t, scheduling_delay, decision_window):
    value, age, _ = store.read(t, scheduling_delay)
    return value, age, decision_window - age


def arm_b0_multi(stores: Dict[str, LaggedFeatureStore], t: float,
                  scheduling_delay: float, decision_windows: Dict[str, float]) -> MultiReadOutcome:
    served, ages, budgets, dropped = {}, {}, {}, {}
    for name, store in stores.items():
        value, age, budget = _budget(store, name, t, scheduling_delay, decision_windows[name])
        served[name], ages[name], budgets[name], dropped[name] = value, age, budget, False
    return MultiReadOutcome(served, ages, budgets, dropped, request_latency=0.0)


def arm_t1_multi(stores: Dict[str, LaggedFeatureStore], t: float,
                  scheduling_delay: float, decision_windows: Dict[str, float],
                  poll_interval: float = 0.5, timeout: float = 10.0) -> MultiReadOutcome:
    waited = 0.0
    while waited < timeout:
        served, ages, budgets, dropped = {}, {}, {}, {}
        all_in_budget = True
        for name, store in stores.items():
            value, age, budget = _budget(store, name, t + waited, scheduling_delay, decision_windows[name])
            served[name], ages[name], budgets[name], dropped[name] = value, age, budget, False
            if budget < 0:
                all_in_budget = False
        if all_in_budget:
            return MultiReadOutcome(served, ages, budgets, dropped, request_latency=waited)
        waited += poll_interval
    # timed out: serve whatever's available for everyone
    return MultiReadOutcome(served, ages, budgets, dropped, request_latency=waited, timed_out=True)


import math

def arm_t2_multi(stores: Dict[str, LaggedFeatureStore], t: float,
                  scheduling_delay: float, decision_windows: Dict[str, float],
                  fallback_strategy: str = "last_trusted",
                  decay_half_life: float = 30.0) -> MultiReadOutcome:
    """
    fallback_strategy:
      - "last_trusted": carry forward the last in-budget value verbatim
        (original behavior).
      - "decay": exponentially decay the last trusted value toward 0 as
        time since it was trusted grows (half-life = decay_half_life
        seconds). Models "confidence fades" rather than a hard cliff.
      - "extrapolate": linear extrapolation from the last two trusted
        points. Models "the trend probably continued." Clipped to >= 0.
    """
    served, ages, budgets, dropped = {}, {}, {}, {}
    for name, store in stores.items():
        value, age, budget = _budget(store, name, t, scheduling_delay, decision_windows[name])
        if budget >= 0:
            store.mark_trusted(value, t)
            served[name], dropped[name] = value, False
        else:
            served[name] = _fallback_value(store, t, fallback_strategy, decay_half_life)
            dropped[name] = True
        ages[name], budgets[name] = age, budget
    return MultiReadOutcome(served, ages, budgets, dropped, request_latency=0.0)


def _fallback_value(store: LaggedFeatureStore, t: float, strategy: str, decay_half_life: float):
    if store.last_trusted_value is None:
        return DEFAULT_SAFE_VALUE

    if strategy == "last_trusted":
        return store.last_trusted_value

    if strategy == "decay":
        last_t, last_v = store.trusted_history[-1]
        elapsed = max(t - last_t, 0.0)
        decay_factor = 0.5 ** (elapsed / decay_half_life)
        return last_v * decay_factor

    if strategy == "extrapolate":
        if len(store.trusted_history) < 2:
            return store.last_trusted_value
        (t0, v0), (t1, v1) = store.trusted_history
        if t1 == t0:
            return v1
        slope = (v1 - v0) / (t1 - t0)
        predicted = v1 + slope * (t - t1)
        return max(predicted, 0.0)

    raise ValueError(f"unknown fallback_strategy: {strategy}")


ARMS_MULTI = {
    "B0": arm_b0_multi,
    "T1": arm_t1_multi,
    "T2": arm_t2_multi,
}
