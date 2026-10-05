# Feature Freshness Budgets

Reproducible research code for the paper:

> **Feature Freshness Budgets for Real-Time ML Inference Under Stream Lag**
> Amit Rajula. Preprint: [arXiv:2610.02259](https://arxiv.org/abs/2610.02259).
> Under review at *The VLDB Journal*.

The paper models an online feature store as a materialized view over an event
stream, defines a **per-feature freshness budget**, proves a staleness bound
and a closed-form collapse threshold, and validates both in simulation and on
a real Apache Kafka + Redis + PostgreSQL pipeline. Every table and figure in
the paper regenerates from the code here; all runs are seeded and
deterministic.

## Repository layout

```
simulation/            Pure discrete-event simulation (no external services)
  sim_core.py            event generator, oracle store, lagged store, Prop. 1 bound
  arms.py / arms_multi.py  the three policies (B0 baseline, T1 block-on-slowest, T2 budgeted)
  run_experiment*.py     single-scenario runners (single- and multi-feature)
  run_matrix*.py         parameter sweeps
  fine_sweep.py          0.40-0.65 sweep that resolves the Proposition 2 thresholds
  sweep_t1_params.py     T1 timeout/poll sweep
  sweep_fallback_strategy.py  T2 fallback comparison (last_trusted / decay / extrapolate)
  contention.py / run_load_experiment.py  load-induced collapse via a queueing model
  analyze*.py            aggregation into the paper's summary tables
  *_summary.csv          precomputed summary outputs

infra/                 Real-infrastructure validation (Kafka + Redis + PostgreSQL)
  docker-compose.yml     the three backing services
  producer.py            emits events to Kafka, dual-writes ground truth to PostgreSQL
  feature_consumer.py    maintains windowed features in Redis on a time-triggered cadence
  scorer.py              issues decision requests, scores against the PostgreSQL oracle
  analyze_infra_runs.py  builds the real-infra threshold table
  README-infra.md        setup, run order (shared --epoch), and debugging notes

sim_infra_comparison/  Reconciling simulation with infrastructure
  sim_vs_infra_latency.py    constant-latency hypothesis
  sim_vs_infra_variance.py   stochastic-latency hypothesis
  sim_vs_infra_pushmodel.py  sampling-phase model (the one that reconciles to MAE 0.018)
  README.md                  the hypothesis chain and the finding
```

## Quick start (simulation only — no Docker needed)

```bash
cd simulation
pip install -r requirements.txt
python3 run_matrix_multi.py && python3 analyze_multi.py   # multi-feature collapse
python3 fine_sweep.py && python3 analyze_fine.py          # Proposition 2 thresholds
```

## Real-infrastructure runs

Requires Docker. See `infra/README-infra.md` for the full procedure. The three
processes must share a common `--epoch` (a wall-clock time origin); the scorer
flags runs where this was not done. Treat first runs as debugging.

## Key results reproduced here

- Proposition 2 collapse threshold at budget multiplier m ≈ 0.50
  (predicted min_f m*_f = 0.5012), in both simulation and real infrastructure.
- The dominant simulation-to-infrastructure gap is a **sampling-phase
  artifact**, not latency: randomizing the phase between sampling and
  recomputation reconciles the two to 0.018 mean absolute error in per-feature
  drop rate (`sim_infra_comparison/`).

## Citation

See `CITATION.cff`. In the meantime:

```bibtex
@misc{rajula2026feature,
  title  = {Feature Freshness Budgets for Real-Time ML Inference Under Stream Lag},
  author = {Rajula, Amit},
  year   = {2026},
  eprint = {2610.02259},
  archivePrefix = {arXiv},
  primaryClass  = {cs.DC},
  doi    = {10.48550/arXiv.2610.02259}
}
```

## License

MIT — see `LICENSE`.
