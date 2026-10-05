"""
Feature Freshness Budgets — simulation core.

Models a single entity's transaction stream and a single velocity feature
("txn_count_5min": count of transactions for that entity in the trailing
5-minute window). Two views of the feature are maintained:

  - OracleFeatureStore: the TRUE value as of the current simulation clock,
    computed with zero lag directly from the full event log. This is the
    ground truth a request would see if served instantly and perfectly.

  - LaggedFeatureStore: the SERVED value, i.e. what an inference request
    actually reads. It recomputes on a cadence (aggregation_window) and
    only reflects events up to (now - ingestion_lag). A read additionally
    incurs scheduling_delay (time to fetch/serve).

Proposition 1 (staleness bound):
    age(f, t) <= ingestion_lag + aggregation_window + scheduling_delay
This is asserted on every read in run_experiment.py, not here, so a
violation surfaces as a hard failure in every run rather than being
silently possible.
"""

import random
from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass
class FeatureSpec:
    """
    One feature in a multi-feature decision request. Features are
    deliberately given heterogeneous lag/cadence/thresholds — that
    heterogeneity is what makes T1 (block on slowest) and T2 (bounded,
    per-feature) actually diverge.
    """
    name: str
    rate_per_sec: float
    threshold: int
    window: float = 300.0
    ingestion_lag: float = 5.0
    aggregation_window: float = 5.0
    burst_count: int = 8


@dataclass
class Event:
    entity_id: str
    ts: float          # simulation time the event occurred (seconds)
    amount: float


@dataclass
class SimClock:
    now: float = 0.0


class EventGenerator:
    """
    Poisson background arrivals for an entity, plus optional injected
    bursts (rapid clusters of events) to simulate a fraud-like spike.
    Deterministic given a seed.
    """

    def __init__(self, entity_id: str, rate_per_sec: float, seed: int,
                 burst_starts: List[float] = None, burst_count: int = 0,
                 burst_span: float = 2.0):
        self.entity_id = entity_id
        self.rate = rate_per_sec
        self.rng = random.Random(seed)
        self.burst_starts = burst_starts or []
        self.burst_count = burst_count
        self.burst_span = burst_span

    def generate(self, horizon: float) -> List[Event]:
        events = []
        t = 0.0
        while t < horizon:
            # exponential inter-arrival for background rate
            t += self.rng.expovariate(self.rate) if self.rate > 0 else horizon
            if t < horizon:
                events.append(Event(self.entity_id, t, self.rng.uniform(5, 500)))

        for b_start in self.burst_starts:
            for _ in range(self.burst_count):
                ts = b_start + self.rng.uniform(0, self.burst_span)
                if ts < horizon:
                    events.append(Event(self.entity_id, ts, self.rng.uniform(5, 500)))

        events.sort(key=lambda e: e.ts)
        return events


class OracleFeatureStore:
    """Exact sliding-window count as of any query time. Ground truth."""

    def __init__(self, events: List[Event], window: float = 300.0):
        self.events = events  # sorted by ts
        self.window = window

    def query(self, t: float) -> int:
        lo = t - self.window
        # linear scan is fine at this event volume; swap for bisect if needed
        return sum(1 for e in self.events if lo <= e.ts <= t)


class LaggedFeatureStore:
    """
    Served feature value. Recomputes on a cadence and only sees events up
    to (now - ingestion_lag). Tracks the watermark so callers can compute
    the age of whatever value is currently "in the store".
    """

    def __init__(self, events: List[Event], window: float,
                 ingestion_lag: float, aggregation_window: float):
        self.events = events
        self.window = window
        self.ingestion_lag = ingestion_lag
        self.aggregation_window = aggregation_window
        self._last_compute_t = None
        self._last_value = 0
        self._last_watermark = None
        # last value that was itself served while IN budget — a real
        # "last known good" cache value, used as T2's fallback instead of
        # a hard default. None until the first in-budget read occurs.
        self.last_trusted_value = None
        # last two (time, value) trusted points, for extrapolation fallback
        self.trusted_history = []  # list of (t, value), newest last

    def mark_trusted(self, value, t=None):
        """Call this after a read that was within its decision budget."""
        self.last_trusted_value = value
        if t is not None:
            self.trusted_history.append((t, value))
            if len(self.trusted_history) > 2:
                self.trusted_history.pop(0)

    def _recompute_if_due(self, t: float):
        # Recompute cadence: aggregation_window. First compute happens at t=0.
        if (self._last_compute_t is None) or (t - self._last_compute_t >= self.aggregation_window):
            watermark = t - self.ingestion_lag
            lo = watermark - self.window
            value = sum(1 for e in self.events if lo <= e.ts <= watermark)
            self._last_value = value
            self._last_watermark = watermark
            self._last_compute_t = t

    def read(self, t: float, scheduling_delay: float) -> Tuple[int, float, float]:
        """
        Returns (served_value, age, watermark) as observed at read time
        t + scheduling_delay (the request pays the scheduling delay before
        it sees a value).
        """
        self._recompute_if_due(t)
        read_time = t + scheduling_delay
        age = read_time - (self._last_watermark if self._last_watermark is not None else 0.0)
        return self._last_value, max(age, 0.0), self._last_watermark
