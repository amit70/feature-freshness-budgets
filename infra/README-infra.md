# Real-Infra Validation Scaffold (Kafka + Redis + Postgres)

**This has NOT been executed** -- it was written in a sandbox with no
Docker daemon available. It's a careful, direct port of the simulation's
semantics onto real infrastructure, meant to validate the simulation's
core claims (Proposition 1's bound, the error-rate-vs-multiplier shape,
the Proposition 2 collapse) rather than to be a production system. Treat
first runs as debugging sessions, not results to cite.

## What it mirrors

- `producer.py` <-> `sim_core.EventGenerator`: same rate/burst logic, but
  against wall-clock time (compressed by `--speed`) instead of a
  discrete-event clock. Dual-writes to Kafka (for the consumer) and
  Postgres (for the oracle).
- `feature_consumer.py` <-> `sim_core.LaggedFeatureStore`: recomputes a
  windowed count on the feature's own `aggregation_window` cadence, only
  including events older than `ingestion_lag`, and writes the result to
  Redis with a watermark -- exactly the batch-recompute semantics the
  simulation assumes, not continuous streaming.
- `scorer.py` <-> `run_experiment_multi.run()`: issues a decision
  request every simulated second, reads Redis (served) and Postgres
  (oracle, zero-lag by construction), and applies B0 or T2's budget
  logic. T1 (blocking) isn't implemented here yet -- it needs a polling
  loop against Redis, which is a small addition if you want it.

## Setup

    cd infra
    docker compose up -d
    pip install -r requirements-infra.txt --break-system-packages

Wait a few seconds for Kafka to finish leader election before starting
the producer (check `docker compose logs kafka` if the producer can't
connect).

## Run order (three separate terminals)

**All three processes must share the same sim-time origin (`--epoch`) and
must run concurrently.** This is the single most important thing to get
right -- see the clock-desync note below for why.

Terminal 1 -- producer. It prints the epoch to use:

    python3 producer.py --speed 10 --horizon 600
    # prints e.g.:  [epoch] pass this to the other two processes:  --epoch 1790000000.000

Terminal 2 -- consumer, with that exact epoch, started immediately:

    python3 feature_consumer.py --speed 10 --epoch 1790000000.000

Terminal 3 -- scorer, same epoch, started right after the consumer:

    python3 scorer.py --speed 10 --horizon 600 --multiplier 0.7 --epoch 1790000000.000 --arm T2

To run several multipliers against ONE producer stream, keep terminals 1
and 2 running and fire the scorer repeatedly in terminal 3 with the same
epoch -- but only while the producer is still within its horizon (at
speed 10, that's the first ~60 wall-clock seconds). Past that, sim-time
has run off the end of the horizon and the scorer has nothing live to
read. For clean per-multiplier runs, it's simplest to restart all three
together for each multiplier, each with a fresh epoch.


## What to check first

1. **Does Proposition 1 hold in practice?** Compare each served feature's
   age (Redis `watermark` vs. read time) against the theoretical bound
   `ingestion_lag + aggregation_window + scheduling_delay`. It should --
   if it doesn't, that's either clock drift between the three processes
   or a real scheduling-delay source (Kafka consumer lag, Redis
   round-trip) the pure simulation doesn't model, which would itself be
   worth reporting.
2. **Does the multiplier=0.7 error rate roughly match the simulation's
   0.4%?** Run `scorer.py --arm T2 --multiplier 0.7` a few times and
   compare to `matrix_multi_summary.csv`.
3. **Does the Proposition 2 collapse reproduce?** Run at multiplier=0.45
   (predicted permanent collapse) vs 0.55 (predicted recovery) and check
   whether `n_dropped` behaves the same way.

## First real run — diagnosis and fix (2026-09-25)

A first full run at multiplier=0.7 produced 600 requests with an overall
1.0% error rate (close to the simulation's predicted 0.4%), but
`n_dropped` climbed monotonically from t=347 (0->1->2->3->4->5) and got
permanently stuck at 5/5 for the last 165 consecutive requests
(t=435-599) -- it never recovered. That does NOT match Proposition 2's
prediction at multiplier=0.7 (recovery threshold 0.6008), which says
every feature should periodically refresh. A one-way ratchet that never
resets is not staleness cycling; it's something actually broken.

**Root cause:** `feature_consumer.py`'s recompute was event-triggered,
not time-triggered. The original loop only checked "is this feature due
for recompute" *inside* the per-message handler -- so a feature only
ever got a chance to refresh when a NEW event for that specific feature
arrived. `sim_core.LaggedFeatureStore` (what this is supposed to mirror)
recomputes based on elapsed time whenever it's *read*, independent of
whether a new event just landed. If a low-rate feature (`geo_velocity_10min`
at 0.02 events/sec) goes quiet for a stretch -- which naturally happens
once the mid-run burst passes -- its Redis watermark simply froze
forever, its age grew unbounded, and it permanently exceeded budget.
Once all 5 features independently went quiet, the result was a
permanent full collapse that has nothing to do with the freshness
policy being tested.

**Fix applied:** `feature_consumer.py` now uses `consumer.poll()` with a
timeout instead of blocking iteration, and on every loop tick -- whether
or not a new message arrived -- it checks every previously-seen
(entity, feature) pair for recompute-due and updates Redis accordingly.
This decouples recompute cadence from event arrival, matching the
simulation's semantics. If you already ran the old version, re-run with
this fix before trusting any results from the back half of a scenario.

**Also worth knowing:** the run also had two ~10-second gaps in the
scorer's own request timeline (unrelated to the bug above -- most likely
process-scheduling noise from running 3 Python processes + Docker on
one machine). `scorer.py` now prints a warning live when it detects a
gap >2s, so this is visible during the run instead of only in post-hoc
analysis.

**Caution on interpreting error rate from this kind of run:** several
feature windows (e.g. 1hr) exceed the 600s horizon, so once
`true_decision` flips True after the mid-run burst, it likely never
flips back to False for the rest of the run. A frozen/stale served value
that happened to be above-threshold when it froze will keep "agreeing"
with ground truth by luck, not by genuine freshness -- so a low error
rate during a period where features are known to be stuck (like the
165-request collapse above) is NOT evidence the policy is working. Treat
error rate as informative only alongside `n_dropped`, not on its own.

## Second/third real runs — clock desync (2026-09-25)

After the recompute fix, runs at multiplier 0.7, 0.5, and 0.45 all
reported `n_dropped=0` and identical aggregate stats -- Proposition 2's
predicted collapse at m<0.5 did NOT reproduce. The per-feature CSV showed
why: every recorded `age` was ~ -1345 seconds. A negative age is
physically impossible (a served value can't be from the future), so
`budget = decision_window - age` was always hugely positive and nothing
ever dropped, at any multiplier.

**Root cause:** each process computed sim-time as
`(time.time() - wall_start) * speed` with its OWN `wall_start` set when
that process happened to launch. The consumer had been left running from
a much earlier launch while fresh scorer runs started later, so the two
disagreed on t=0 by ~134 wall-clock seconds -> ~1345 sim-seconds at
speed 10. Every age was measured against a clock that disagreed with the
one that wrote the watermark.

**Fix applied:** all three processes now take a shared `--epoch`
(wall-clock Unix timestamp for sim t=0). The producer prints one; pass
the identical value to the consumer and scorer. `scorer.py` also now
hard-flags any read with impossible age (negative or > horizon) and
prints `[ERROR] ... results are INVALID` so this can never again pass
silently as `n_dropped=0`. The earlier m0.5/m0.45 CSVs are invalid and
should be discarded; re-run with a shared epoch.

## Troubleshooting



**"Ports are not available: ... 0.0.0.0:5432"** — something else on your
machine already owns port 5432 (commonly Postgres.app, Homebrew
postgres, or a leftover container from another project). This scaffold
now maps the container's Postgres to host port **5433** instead
(`docker-compose.yml` and both `PG_DSN` strings are already updated) --
if you still hit a conflict, check `lsof -i :5433` or change the port
mapping again and update `PG_DSN` in both `producer.py` and `scorer.py`
to match.

**psycopg2-binary fails to build ("pg_config executable not found")** —
this happens when pip has no prebuilt wheel for your Python version and
falls back to building from source. This scaffold now uses **`psycopg`
(v3)** instead of `psycopg2-binary`, which has much broader prebuilt-wheel
coverage across Python versions and platforms (including Apple Silicon)
and avoids this class of failure. If you still hit a build error, check
`python3 --version` — Python 3.13+ occasionally still lacks wheels for
some packages; a 3.11/3.12 virtualenv is the safest fallback.

**Verify Docker containers actually started before running the Python
scripts:**

    docker compose ps
    docker compose logs kafka | tail -20    # confirm Kafka finished leader election
    docker compose logs postgres | tail -20 # confirm init.sql ran without error



- T1 (blocking) isn't implemented in `scorer.py` yet.
- No multi-entity load test here -- `run_load_experiment.py`'s queueing
  model isn't ported; Kafka/Redis have their own real queueing behavior
  under load that would need its own instrumentation.
- The producer's event-scheduling loop (`time.sleep(0.05)`) limits
  timing precision at high `--speed`; keep `--speed <= 20` or events
  will bunch up.
- Postgres oracle queries do a full table scan per feature per request
  without care taken for indexing beyond the one composite index in
  `init.sql` -- fine at this data volume, would need tuning at scale.
