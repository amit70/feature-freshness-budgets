# Simulation-to-Infrastructure Reconciliation

How the pure simulation's per-feature drop rates were reconciled with the
real Kafka+Redis+Postgres runs, and what the gap turned out to be. Three
scripts, run in this order, each testing one hypothesis. Requires pandas
and the harness core files (run_experiment_multi.py, sim_core.py,
arms_multi.py) in the parent directory.

## Inputs

- `infra_threshold_summary.csv`, `infra_perfeature_summary.csv` — the real
  infra numbers, produced by `infra/analyze_infra_runs.py` over valid
  (shared-epoch) scorer runs at multipliers 0.45 / 0.50 / 0.55 / 0.70.

## The chain of hypotheses

**1. `sim_vs_infra_latency.py` — constant latency offset.**
Hypothesis: the ~0.3s mean age-excess measured in infra explains why real
infra drops more than the zero-latency sim. Adds a constant 0.3s to served
age (nominal decision_window unchanged). Result: closed ~half the gap
(mean |sim-infra| 0.112 -> 0.063) and fully reconciled the tight-budget /
short-window features at m=0.55, but left a residual: velocity_1min @
m=0.70 stayed at 0.000 vs infra's 0.598.

**2. `sim_vs_infra_variance.py` — stochastic latency (mean 0.3, heavy tail).**
Hypothesis: it's the latency *tail*, not the mean, that drops loose-budget
features. Draws per-request latency from a mixture calibrated to the
observed excess stats. Result: moved velocity_1min @ m=0.70 only 0.000 ->
0.086 (nowhere near 0.598) and slightly worsened the aggregate gap
(0.070). Hypothesis rejected — latency variance is not the dominant cause.

**3. `sim_vs_infra_pushmodel.py` — read/recompute phase decoupling. WINNER.**
Instrumenting velocity_1min's age revealed it was CONSTANT at 1.050 in the
sim — because the request clock (1.0s) and its recompute cadence (1.0s)
were exactly phase-locked, so every read sampled the recompute cycle at
the same favorable instant (just after recompute). Real infra has the
consumer and scorer as independent processes with unsynchronized clocks,
so reads sample the WHOLE cycle, and age ranges over [lag, lag+agg]. This
script models that with (a) a push store where recompute is decoupled from
read, and (b) a random phase offset between the recompute grid and the
request clock. Result: mean |sim-infra| = **0.018** across all features
and multipliers, and velocity_1min @ m=0.70 = 0.599 vs infra 0.598 — the
residual fully explained.

## The finding (for the paper)

The dominant simulation-to-infrastructure gap was NOT latency. It was a
sampling-phase artifact: a pure simulation whose request clock and
recompute cadence are phase-locked reads each feature at a fixed,
favorable point in its recompute cycle and therefore systematically
UNDER-observes staleness. Real infrastructure, with independent
sampling and recomputation processes, sees the full staleness range.

Methodological takeaway, transferable beyond this system: **freshness
simulations must randomize the phase between sampling and recomputation,
or they will systematically underestimate staleness.** A small (~0.3s)
mean-latency term accounts for the minor remainder after the phase fix.

## Reproduce

    python3 sim_vs_infra_latency.py     # -> sim_vs_infra_latency.csv
    python3 sim_vs_infra_variance.py    # -> sim_vs_infra_variance.csv
    python3 sim_vs_infra_pushmodel.py   # -> sim_vs_infra_pushmodel.csv

Each prints its per-multiplier comparison table and the mean absolute gap
to real infra. All are seeded and deterministic.
