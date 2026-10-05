"""
The real-infra analog of run_experiment_multi.py. Issues a decision
request every REQUEST_INTERVAL simulation-seconds, reads each feature's
CURRENT served value from Redis (B0), or applies the T1/T2 policies on
top of that, and compares to ground truth computed directly from
Postgres (the real oracle -- an exact COUNT query, zero lag by
construction since it's a direct query of the full log).

This validates the core empirical claim from the simulation (does the
per-feature staleness bound hold in a real system, and does the
error-rate-vs-multiplier shape match) against actual Kafka+Redis+Postgres
behavior rather than in-memory approximations.

Usage:
    python3 scorer.py --speed 10 --horizon 600 --multiplier 0.7 --arm T2
"""

import argparse
import csv
import json
import time

import psycopg
import redis

PG_DSN = "dbname=ffb user=ffb password=ffb host=localhost port=5433"
REQUEST_INTERVAL = 1.0  # simulation-seconds
SCHEDULING_DELAY = 0.05

FEATURE_PARAMS = {
    "velocity_1min":          {"window": 60.0,   "ingestion_lag": 1.0,  "aggregation_window": 1.0,  "threshold": 2},
    "velocity_5min":          {"window": 300.0,  "ingestion_lag": 2.0,  "aggregation_window": 2.0,  "threshold": 5},
    "velocity_1hr":           {"window": 3600.0, "ingestion_lag": 5.0,  "aggregation_window": 5.0,  "threshold": 15},
    "merchant_diversity_1hr": {"window": 3600.0, "ingestion_lag": 10.0, "aggregation_window": 10.0, "threshold": 8},
    "geo_velocity_10min":     {"window": 600.0,  "ingestion_lag": 15.0, "aggregation_window": 10.0, "threshold": 3},
}


def oracle_query(cur, entity, feature, now):
    params = FEATURE_PARAMS[feature]
    lo = now - params["window"]
    cur.execute(
        "SELECT COUNT(*) FROM events WHERE entity_id=%s AND feature_name=%s "
        "AND event_ts BETWEEN %s AND %s",
        (entity, feature, lo, now),
    )
    return cur.fetchone()[0]


def read_served(r, entity, feature, now):
    key = f"feature:{entity}:{feature}"
    raw = r.get(key)
    if raw is None:
        return 0, now  # cold start, maximally stale
    doc = json.loads(raw)
    age = now - doc["watermark"]
    return doc["value"], age


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--speed", type=float, default=10.0)
    p.add_argument("--horizon", type=float, default=600.0)
    p.add_argument("--multiplier", type=float, default=0.7)
    p.add_argument("--arm", type=str, default="B0", choices=["B0", "T2"])  # T1 needs polling; add if desired
    p.add_argument("--entity", type=str, default="E1")
    p.add_argument("--redis-host", type=str, default="localhost")
    p.add_argument("--out", type=str, default=None)
    p.add_argument("--epoch", type=float, required=True,
                   help="shared wall-clock Unix epoch (t=0) printed by producer.py. "
                        "MUST match the producer's and consumer's value.")
    args = p.parse_args()

    if args.out is None:
        args.out = f"infra_run_m{args.multiplier}.csv"

    r = redis.Redis(host=args.redis_host, port=6379, decode_responses=True)
    pg = psycopg.connect(PG_DSN)
    cur = pg.cursor()

    decision_windows = {
        name: args.multiplier * (p["ingestion_lag"] + p["aggregation_window"] + SCHEDULING_DELAY)
        for name, p in FEATURE_PARAMS.items()
    }
    # Physical worst-case bound per feature (Proposition 1), independent of
    # the multiplier -- age should never exceed this in a correct system.
    prop1_bound = {
        name: p["ingestion_lag"] + p["aggregation_window"] + SCHEDULING_DELAY
        for name, p in FEATURE_PARAMS.items()
    }

    last_trusted = {}
    wall_start = args.epoch

    def sim_time():
        return (time.time() - wall_start) * args.speed

    # Guard: if the epoch is stale, sim_time() is already past the horizon
    # (or wildly negative) and the loop below would never run, producing an
    # empty file. Catch it up front with an actionable message. A valid
    # epoch is roughly "now" in wall-clock terms; sim_time() at start should
    # be a small number, not already past the horizon or negative.
    start_sim = sim_time()
    if start_sim >= args.horizon or start_sim < -1.0:
        raise SystemExit(
            f"[ERROR] --epoch {args.epoch} is stale: sim-time is already "
            f"{start_sim:.1f}s at startup (horizon={args.horizon}s). The epoch "
            f"must be the CURRENT wall-clock value that producer.py printed at "
            f"launch, not a placeholder or an old run's value. Start the producer "
            f"now, copy its printed --epoch, and use that same value here."
        )

    rows = []
    feature_rows = []
    prop1_violations = 0
    clock_desync_flag = [0]
    next_request = 0.0
    last_now = 0.0
    while sim_time() < args.horizon:
        now = sim_time()
        if now < next_request:
            time.sleep(0.02)
            continue
        if now - last_now > 2.0 and last_now > 0:
            print(f"  [warning] {now - last_now:.1f}s gap in request timeline "
                  f"({last_now:.2f} -> {now:.2f}) -- likely a scheduling stall, "
                  f"not a data issue")
        last_now = now
        next_request += REQUEST_INTERVAL

        true_decision = False
        served_decision = False
        n_dropped = 0
        for feature, params in FEATURE_PARAMS.items():
            true_val = oracle_query(cur, args.entity, feature, now)
            true_over = true_val >= params["threshold"]
            if true_over:
                true_decision = True

            served_val, age = read_served(r, args.entity, feature, now)
            budget = decision_windows[feature] - age

            # Sanity: age must be >= 0 (a served value can't be from the
            # future) and shouldn't wildly exceed the horizon. A large
            # negative age means the consumer and scorer disagree on t=0
            # (different --epoch) -- the exact clock-desync bug that made an
            # earlier run silently report n_dropped=0 everywhere.
            if age < -1.0 or age > args.horizon:
                clock_desync_flag[0] += 1

            # Proposition 1 check against the physical bound. Real infra can
            # add scheduling/Kafka/Redis latency the pure sim doesn't model,
            # so we RECORD violations rather than assert-crash -- a violation
            # is itself a finding (the real bound is looser than the model's).
            over_bound = age > prop1_bound[feature] + 1e-6
            if over_bound:
                prop1_violations += 1

            dropped = False
            if args.arm == "B0" or budget >= 0:
                use_val = served_val
                if budget >= 0:
                    last_trusted[feature] = served_val
            else:  # T2 over-budget: fall back to last trusted
                use_val = last_trusted.get(feature, 0)
                n_dropped += 1
                dropped = True

            served_over = use_val >= params["threshold"]
            if served_over:
                served_decision = True

            feature_rows.append({
                "t": round(now, 2), "feature": feature,
                "true_val": true_val, "served_val": served_val,
                "use_val": use_val, "age": round(age, 3),
                "prop1_bound": prop1_bound[feature],
                "over_prop1_bound": over_bound,
                "budget": round(budget, 3), "dropped": dropped,
                "true_over": true_over, "served_over": served_over,
                "contributes_error": served_over != true_over,
            })

        rows.append({
            "t": round(now, 2), "arm": args.arm, "multiplier": args.multiplier,
            "true_decision": true_decision, "served_decision": served_decision,
            "error": served_decision != true_decision, "n_dropped": n_dropped,
        })

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)

    feature_out = args.out.replace(".csv", "_perfeature.csv")
    if feature_out == args.out:
        feature_out = args.out + ".perfeature.csv"
    with open(feature_out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=feature_rows[0].keys())
        w.writeheader()
        w.writerows(feature_rows)

    n_err = sum(r["error"] for r in rows)
    print(f"[{args.arm}] multiplier={args.multiplier} requests={len(rows)} "
          f"errors={n_err} prop1_violations={prop1_violations}")
    print(f"  -> {args.out}")
    print(f"  -> {feature_out} (per-feature detail)")
    if clock_desync_flag[0]:
        print(f"  [ERROR] {clock_desync_flag[0]} reads had impossible age "
              f"(negative or > horizon) -- the consumer and scorer are NOT using "
              f"the same --epoch. Results are INVALID. Restart all three processes "
              f"with the same --epoch value (printed by producer.py).")
    if prop1_violations:
        print(f"  [note] {prop1_violations} reads exceeded the Proposition 1 "
              f"bound -- real infra latency beyond the modeled bound; inspect "
              f"the per-feature CSV (over_prop1_bound=True rows).")


if __name__ == "__main__":
    main()
