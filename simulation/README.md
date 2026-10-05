# Feature Freshness Budgets — Simulation Harness

Companion code for "Feature Freshness Budgets for Real-Time ML Inference
Under Stream Lag." A discrete-event simulation (no Kafka/Docker required)
that models a feature store as a materialized view over a transaction
stream and compares three read policies against it.

## Requirements

    pip install pandas --break-system-packages   # or use a venv

Python 3.9+, standard library only otherwise (random, csv, dataclasses,
argparse). No external services.

## What's here

**Single-feature harness** (one velocity feature, proves the mechanics):
- `sim_core.py` — event generator, oracle (ground-truth) feature store,
  lagged (served) feature store. `FeatureSpec` here is used by the
  multi-feature harness.
- `arms.py` — three read policies: B0 (baseline, no budget check),
  T1 (block whole request until in-budget, timeout fallback),
  T2 (per-feature budget, drop-and-impute if over budget).
- `run_experiment.py` — one scenario, one config, all three arms,
  asserts Proposition 1 on every read.
- `run_matrix.py` — sweeps ingestion_lag x aggregation_window x
  decision_window x burst_count x 3 seeds x 3 arms.
- `analyze.py` — aggregates `matrix_results.csv` into summary tables.

**Multi-feature harness** (5 heterogeneous features — this is where T1
and T2 actually diverge; see "Key finding" below):
- `arms_multi.py` — same three policies, but T1 now blocks on the
  SLOWEST of K features, and T2 drops only the offending features
  (falling back to each feature's own last-trusted/last-known-good
  value, not a hard default).
- `run_experiment_multi.py` — 5 features spanning 1s-to-15s lag,
  decision = OR across features. `decision_window_multiplier` scales
  every feature's own staleness bound uniformly (1.0 = as tight as the
  bound allows, <1.0 = tighter than the bound, i.e. an aggressive SLA).
- `run_matrix_multi.py` — sweeps multiplier in
  {0.3, 0.5, 0.7, 0.9, 1.2, 1.5, 2.0, 3.0} x 5 seeds x 3 arms.
- `analyze_multi.py` — the frontier table (error rate, T1's added
  latency, T2's dropped-feature count, all vs. multiplier).

## How to run

Quick single run (sanity check, ~1 second):

    python3 run_experiment.py --seed 1 --ingestion_lag 5 \
        --aggregation_window 5 --decision_window 8 --out run_single.csv

Full single-feature matrix (~2 seconds, 194K rows):

    python3 run_matrix.py
    python3 analyze.py

Multi-feature single run:

    python3 run_experiment_multi.py --seed 1 \
        --decision_window_multiplier 0.9 --out run_multi.csv

Full multi-feature matrix (~10 seconds, 72K rows) — this is the one
that produces the headline result:

    python3 run_matrix_multi.py
    python3 analyze_multi.py

All runs are deterministic given `--seed`; re-running with the same
seed reproduces identical output.

## Key finding so far (multi-feature matrix, 5 seeds)

There is a sharp collapse boundary for T2 between `multiplier=0.5` and
`multiplier=0.7`:

| multiplier | T1 error | T1 latency | T2 error | T2 dropped/5 |
|---|---|---|---|---|
| 0.3 | 2.4% | 9.0s | **92.2%** | 5.0 |
| 0.5 | 1.7% | 4.0s | **92.2%** | 5.0 |
| 0.7 | 1.2% | 0.9s | 0.4% | 2.4 |
| 0.9 | 0.3% | 0.3s | 0.4% | 0.6 |
| >=1.2 | 0.4% | 0.0s | 0.4% | 0.0 |

Below the boundary, T2 can never establish a trusted fallback value for
its slower features (the budget is tighter than the feature can ever
refresh within), so it permanently serves the cold-start default and
error rate collapses to ~92%. T1 degrades gracefully instead — latency
grows smoothly as the budget tightens, but error never exceeds 2.4%
even at the tightest setting tested.

This is the asymmetry the paper should lead with: bounded-staleness
(T2) is a good trade only away from its collapse boundary; near or
below it, blocking (T1) is strictly better despite its latency cost.

## Proposition 2 — closed-form permanent-drop threshold

A feature *f* can never be served in-budget under a uniform
`decision_window_f = m . bound_f` policy when

    m < m*_f = (ingestion_lag_f + scheduling_delay) / (ingestion_lag_f + aggregation_window_f + scheduling_delay)

The minimum achievable age for any served read is `ingestion_lag_f +
scheduling_delay` (the instant right after a recompute); if even that
floor exceeds the decision window, no read at any point in the feature's
refresh cycle can land in-budget, and it is permanently stuck at its
cold-start/last-trusted fallback.

For the 5 default features, `m*_f` ranges from 0.5012
(`merchant_diversity_1hr`) to 0.6008 (`geo_velocity_10min`, the one
feature built with `lag != aggregation_window`). Because the decision
rule is OR across features, two distinct system-level thresholds follow:

- `min_f(m*_f) = 0.5012` — the point where the EASIEST feature recovers,
  which is enough for OR-combined accuracy to start catching bursts
  again.
- `max_f(m*_f) = 0.6008` — the point where the LAST feature recovers,
  which is what drives full completeness (`mean_n_dropped -> 0`).

`fine_sweep.py` sweeps `decision_window_multiplier` from 0.40 to 0.65 in
steps of 0.01 (5 seeds x 3 arms, 234,000 rows) to resolve both
transitions directly against these predictions:

| multiplier | T2 error rate | T2 mean_n_dropped |
|---|---|---|
| 0.40 - 0.50 | 92.2% (flat) | 5.0 (flat) |
| **0.51** | **10.3%** | 4.2 |
| 0.52 - 0.60 | 0.4% (flat) | 3.1 - 3.2 |
| **0.61** | 0.4% | **2.7** (step down) |
| 0.62 - 0.65 | 0.4% (flat) | 2.6 - 2.7 |

Both predicted thresholds land exactly where predicted, to the sweep's
own 0.01 resolution: error collapses between 0.50 and 0.51 (predicted
0.5012), and a second, smaller completeness step appears between 0.60
and 0.61 (predicted 0.6008). Run `fine_sweep.py` then `analyze_fine.py`
to reproduce.



- Proposition 1 (`age <= ingestion_lag + aggregation_window +
  scheduling_delay`) is asserted on every single read, single- and
  multi-feature harnesses alike. It has held across 266,400 total reads
  in the runs used to produce the numbers above — a violation would
  raise an `AssertionError` immediately, not fail silently.
- `decision_window_multiplier >= 1.0` degenerates all three arms to
  identical behavior by construction (budget can never go negative) —
  this was used as a smoke test that the harness wiring is correct.

## Extensions built this session (A/B/C/D)

**(A) T1 timeout/poll sweep** — `sweep_t1_params.py` / results in
`t1_param_sweep_summary.csv`. Surfaced an important subtlety, not just a
tuning result: `error_rate` is currently measured against
`true_decision` evaluated at the ORIGINAL request time `t`, but a longer
T1 timeout serves a value reflecting state at `t + waited`. At the hard
case (multiplier=0.3), error actually INCREASES with timeout (0.03% at
timeout=1s -> 3.7% at timeout=20s) because blocking longer silently
re-anchors the answer to a later moment than the one asked about. This
is worth stating as its own finding (blocking trades staleness for
*temporal displacement*) rather than folding into a simple
latency-vs-accuracy trade-off. Consider adding a second metric
(`error_at_serve_time`) to separate the two effects cleanly before
writing this into the paper.

**(B) Smarter T2 fallback** — `arms_multi.arm_t2_multi` now takes
`fallback_strategy` in `{last_trusted, decay, extrapolate}`; swept in
`sweep_fallback_strategy.py` / `fallback_strategy_summary.csv`. Two
results, both clean:
- `mean_n_dropped` is IDENTICAL across all three strategies at every
  multiplier — confirms Proposition 2's threshold is a property of
  (lag, aggregation_window, scheduling_delay) alone, not of what you do
  once a feature is dropped.
- Below the collapse threshold, all three strategies also converge to
  identical error (no trusted value is ever established, so decay/
  extrapolate degrade to the same cold-start default as last_trusted).
  Above it, "decay" is slightly WORSE than plain carry-forward (0.53%
  vs 0.40% error) for these monotonic count features — decaying toward
  zero introduces false negatives that naive carry-forward avoids.
  Worth a sentence: smarter isn't automatically better; it depends on
  whether the feature's true dynamics actually decay.

**(C) Load-induced contention** — `contention.py` (a deterministic
multi-worker queueing simulation) + `run_load_experiment.py`. The
headline result: at a FIXED multiplier=0.7 (previously safe, 0.4%
error), sweeping entity count against a fixed-capacity shared compute
pool (4 workers x 0.2s/job = 20 jobs/sec capacity) reproduces the exact
Proposition 2 collapse signature purely from load, once arrival rate
crosses capacity (200->300 entities, utilization crossing 1.0):
- T2: error 0.4%->92.2%, mean_n_dropped 2.4->5.0 (identical to the
  Proposition-2-driven collapse, but triggered by queueing delay, not a
  changed decision_window).
- T1: degrades gracefully instead (error 1.2%->2.5%, latency saturates
  at its 10s timeout rather than diverging).
This ties the paper's freshness-budget theory directly to ordinary
queueing-theory utilization (`arrival_rate/service_rate = 1`) as the
predictor of collapse — "your SLA was fine yesterday, nothing changed
except traffic" stated formally. Results in `load_experiment_summary.csv`
and `load_queue_delay.csv`.

**(D) Real-infra scaffold** — `infra/` (Docker Compose: Kafka +
Zookeeper + Redis + Postgres, plus `producer.py`, `feature_consumer.py`,
`scorer.py`). Written but NOT executed in this session (no Docker
available in the build sandbox) — syntax-checked only. See
`infra/README-infra.md` for setup, run order, and what to check first.
Treat first runs as debugging, not citable results, until you've
validated Proposition 1 holds against real Kafka consumer lag / Redis
round-trip behavior, which the pure simulation doesn't model.

## Known limitations / what's not modeled yet

- Single entity per run (no cross-entity contention or fan-out).
- T1's blocking timeout (10s) and poll interval (0.5s) are fixed;
  worth sweeping.
- T2's fallback is "last trusted value," not a decayed/extrapolated
  estimate — a smarter fallback might push the collapse boundary lower.
- No real trained model in the loop — the decision rule is a fixed
  threshold (OR across features), which isolates staleness as the only
  variable but doesn't speak to ML accuracy degradation directly.
- Results are simulation-based; no live infrastructure (Kafka/Redis)
  validation yet. Paper 1's Docker Compose setup (Kafka + Redis) could
  be reused for that as a follow-up.
