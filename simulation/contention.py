"""
Everything so far assumed a dedicated compute pipeline per feature. In
production, one shared feature-computation service serves many entities'
recomputes. Under load, recompute jobs queue for workers, and that
queueing delay is EXTRA staleness on top of the nominal
ingestion_lag/aggregation_window -- this is the "peak traffic makes
features stale" failure mode operators actually see.

This module simulates that queueing layer with a simple work-conserving
multi-worker queue (like an M/G/c system, but deterministic given a
seed) and reports the queueing delay distribution. `run_load_experiment.py`
then feeds that delay into the existing LaggedFeatureStore machinery as
additional scheduling_delay, to see whether load alone can push a
feature across its own Proposition 2 threshold.
"""

import random
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class QueueResult:
    arrival: float
    start: float
    finish: float
    queue_delay: float  # finish - arrival - processing_time


def simulate_shared_queue(arrival_times: List[float], processing_time: float,
                           n_workers: int) -> List[QueueResult]:
    """
    Work-conserving simulation: each arriving job is assigned to whichever
    worker becomes free soonest. Deterministic given arrival_times.
    """
    worker_free = [0.0] * n_workers
    results = []
    for at in sorted(arrival_times):
        idx = min(range(n_workers), key=lambda i: worker_free[i])
        start = max(at, worker_free[idx])
        finish = start + processing_time
        worker_free[idx] = finish
        results.append(QueueResult(at, start, finish, finish - at - processing_time))
    return results


def recompute_arrival_times(n_entities: int, aggregation_window: float,
                             horizon: float, seed: int, jitter: float = 0.1) -> List[float]:
    """
    Each entity's feature recomputes on its own cadence (aggregation_window),
    independently jittered so they don't all land in lockstep (which would
    be unrealistically synchronized). Returns ALL entities' recompute
    arrival times pooled together, as they'd compete for the shared pool.
    """
    rng = random.Random(seed)
    arrivals = []
    for e in range(n_entities):
        offset = rng.uniform(0, aggregation_window)
        t = offset
        while t < horizon:
            jittered = t + rng.uniform(-jitter, jitter)
            arrivals.append(max(jittered, 0.0))
            t += aggregation_window
    return arrivals


def mean_queue_delay(n_entities: int, aggregation_window: float, horizon: float,
                      processing_time: float, n_workers: int, seed: int) -> Tuple[float, float, int]:
    """Returns (mean_delay, p95_delay, n_jobs) for a given load configuration."""
    arrivals = recompute_arrival_times(n_entities, aggregation_window, horizon, seed)
    results = simulate_shared_queue(arrivals, processing_time, n_workers)
    delays = sorted(r.queue_delay for r in results)
    if not delays:
        return 0.0, 0.0, 0
    mean_d = sum(delays) / len(delays)
    p95_d = delays[int(0.95 * (len(delays) - 1))]
    return mean_d, p95_d, len(delays)
