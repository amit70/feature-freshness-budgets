"""
Real-infra event producer. Mirrors sim_core.EventGenerator's logic
(Poisson background rate + an injected burst at the horizon midpoint) but
runs against actual wall-clock time instead of a discrete-event clock, so
this validates the simulation's claims against a real Kafka + Redis +
Postgres pipeline rather than in-memory Python objects.

SPEED_MULTIPLIER compresses wall-clock time relative to "simulation time"
(the timestamps written to events/features) so a 600s scenario doesn't
have to take 10 real minutes -- at SPEED_MULTIPLIER=10, it takes 60s.
Feature semantics (windows, lags) are still defined in simulation-time
seconds, matching the pure-Python harness exactly, so results are
comparable.

Usage:
    python3 producer.py --speed 10 --horizon 600
"""

import argparse
import json
import random
import time

import psycopg
from kafka import KafkaProducer

# Same 5 features as run_experiment_multi.default_feature_specs(), kept
# in sync by hand -- if you change one, change the other.
FEATURE_SPECS = [
    {"name": "velocity_1min", "rate_per_sec": 0.03, "burst_count": 3},
    {"name": "velocity_5min", "rate_per_sec": 0.05, "burst_count": 8},
    {"name": "velocity_1hr", "rate_per_sec": 0.08, "burst_count": 10},
    {"name": "merchant_diversity_1hr", "rate_per_sec": 0.04, "burst_count": 6},
    {"name": "geo_velocity_10min", "rate_per_sec": 0.02, "burst_count": 4},
]

KAFKA_TOPIC = "ffb-events"
PG_DSN = "dbname=ffb user=ffb password=ffb host=localhost port=5433"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--speed", type=float, default=10.0, help="wall-clock compression factor")
    p.add_argument("--horizon", type=float, default=600.0, help="simulation-time seconds")
    p.add_argument("--entity", type=str, default="E1")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--kafka", type=str, default="localhost:9092")
    p.add_argument("--epoch", type=float, default=None,
                   help="shared wall-clock Unix epoch defining sim t=0 for ALL "
                        "three processes. Producer prints one if not given; pass "
                        "the SAME value to feature_consumer.py and scorer.py.")
    args = p.parse_args()

    rng = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.kafka,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    pg = psycopg.connect(PG_DSN)
    pg.autocommit = True
    cur = pg.cursor()

    burst_start = args.horizon * 0.5
    wall_start = args.epoch if args.epoch is not None else time.time()
    print(f"[epoch] sim t=0 anchored to wall-clock {wall_start:.3f}")
    print(f"[epoch] pass this to the other two processes:  --epoch {wall_start:.3f}")

    def sim_time():
        return (time.time() - wall_start) * args.speed

    print(f"Producing for {args.horizon}s simulation-time "
          f"(~{args.horizon / args.speed:.0f}s wall-clock) at speed={args.speed}x")

    # Schedule next background event per feature
    next_event = {f["name"]: rng.expovariate(f["rate_per_sec"]) for f in FEATURE_SPECS}
    burst_fired = {f["name"]: False for f in FEATURE_SPECS}

    while sim_time() < args.horizon:
        now = sim_time()
        for f in FEATURE_SPECS:
            name = f["name"]
            if now >= next_event[name]:
                emit(producer, cur, args.entity, name, now, rng.uniform(5, 500))
                next_event[name] = now + rng.expovariate(f["rate_per_sec"])
            if now >= burst_start and not burst_fired[name]:
                burst_fired[name] = True
                for _ in range(f["burst_count"]):
                    jitter = rng.uniform(0, 2.0)
                    emit(producer, cur, args.entity, name, now + jitter, rng.uniform(5, 500))
        time.sleep(0.05)  # polling granularity; fine-grained enough at speed<=20x

    producer.flush()
    print("Done.")


def emit(producer, cur, entity_id, feature_name, event_ts, amount):
    payload = {"entity_id": entity_id, "feature_name": feature_name,
               "event_ts": event_ts, "amount": amount}
    producer.send(KAFKA_TOPIC, value=payload)
    cur.execute(
        "INSERT INTO events (entity_id, feature_name, event_ts, amount) VALUES (%s, %s, %s, %s)",
        (entity_id, feature_name, event_ts, amount),
    )


if __name__ == "__main__":
    main()
