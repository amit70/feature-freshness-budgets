"""
Three policy arms for consuming a (possibly stale) feature at decision time.

All arms see the same LaggedFeatureStore and the same request arrival time.
They differ only in how they react to staleness.
"""

from dataclasses import dataclass
from sim_core import LaggedFeatureStore


@dataclass
class ReadOutcome:
    served_value: float
    age: float
    budget: float           # decision_window - age; negative = over budget
    added_latency: float    # extra wait incurred by the policy (T1 only)
    dropped: bool            # True if T2 dropped the feature (imputed)
    timed_out: bool = False  # True if T1 hit its wait timeout


DEFAULT_SAFE_VALUE = 0  # T2 imputation fallback: treat as "no signal"


def arm_b0_baseline(store: LaggedFeatureStore, t: float, scheduling_delay: float,
                     decision_window: float) -> ReadOutcome:
    """Serve whatever's in the store. No budget check, no waiting."""
    value, age, _ = store.read(t, scheduling_delay)
    budget = decision_window - age
    return ReadOutcome(value, age, budget, added_latency=0.0, dropped=False)


def arm_t1_block_on_slowest(store: LaggedFeatureStore, t: float, scheduling_delay: float,
                             decision_window: float, poll_interval: float = 0.5,
                             timeout: float = 10.0) -> ReadOutcome:
    """
    Block the request until the feature is within its decision window, or
    give up after `timeout` and serve whatever is available (recorded as
    timed_out=True so the cost of blocking is visible).
    """
    waited = 0.0
    while waited < timeout:
        value, age, _ = store.read(t + waited, scheduling_delay)
        budget = decision_window - age
        if budget >= 0:
            return ReadOutcome(value, age, budget, added_latency=waited, dropped=False)
        waited += poll_interval
    # timed out — serve last known value anyway
    value, age, _ = store.read(t + waited, scheduling_delay)
    budget = decision_window - age
    return ReadOutcome(value, age, budget, added_latency=waited, dropped=False, timed_out=True)


def arm_t2_budgeted(store: LaggedFeatureStore, t: float, scheduling_delay: float,
                     decision_window: float) -> ReadOutcome:
    """
    Serve the feature only if it's within its own budget. If over budget,
    drop it (impute DEFAULT_SAFE_VALUE) and flag as dropped. No waiting.
    """
    value, age, _ = store.read(t, scheduling_delay)
    budget = decision_window - age
    if budget >= 0:
        return ReadOutcome(value, age, budget, added_latency=0.0, dropped=False)
    return ReadOutcome(DEFAULT_SAFE_VALUE, age, budget, added_latency=0.0, dropped=True)


ARMS = {
    "B0": arm_b0_baseline,
    "T1": arm_t1_block_on_slowest,
    "T2": arm_t2_budgeted,
}
