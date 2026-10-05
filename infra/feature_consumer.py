"""
Consumes ffb-events from Kafka, maintains a per-entity sliding window per
feature in memory, and writes the SERVED value to Redis on the feature's
own aggregation_window cadence -- deliberately not on every event, to
match LaggedFeatureStore's batch-recompute semantics rather than
continuous streaming (continuous update would trivially defeat the whole
staleness question).

ingestion_lag is modeled by delaying when an event becomes visible to the
recompute: an event with event_ts is only included in a recompute that
happens at sim_time >= event_ts + ingestion_lag.

Redis key: feature:{entity_id}:{feature_name} ->
    {"value": <int>, "watermark": <float sim-time>, "computed_at": <float sim-time>}

Usage:
    python3 feature_consumer.py --speed 10
"""

import argparse
import json
import time
from collections import defaultdict, deque

import redis
from kafka import KafkaConsumer

KAFKA_TOPIC = "ffb-events"

# Must match producer.py / default_feature_specs() in run_experiment_multi.py
FEATURE_PARAMS = {
    "velocity_1min":          {"window": 60.0,   "ingestion_lag": 1.0,  "aggregation_window": 1.0},
    "velocity_5min":          {"window": 300.0,  "ingestion_lag": 2.0,  "aggregation_window": 2.0},
    "velocity_1hr":           {"window": 3600.0, "ingestion_lag": 5.0,  "aggregation_window": 5.0},
    "merchant_diversity_1hr": {"window": 3600.0, "ingestion_lag": 10.0, "aggregation_window": 10.0},
    "geo_velocity_10min":     {"window": 600.0,  "ingestion_lag": 15.0, "aggregation_window": 10.0},
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--speed", type=float, default=10.0)
    p.add_argument("--kafka", type=str, default="localhost:9092")
    p.add_argument("--redis-host", type=str, default="localhost")
    p.add_argument("--epoch", type=float, required=True,
                   help="shared wall-clock Unix epoch (t=0) printed by producer.py. "
                        "MUST match the producer's and scorer's value.")
    args = p.parse_args()

    consumer = KafkaConsumer(
        KAFKA_TOPIC, bootstrap_servers=args.kafka,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    r = redis.Redis(host=args.redis_host, port=6379, decode_responses=True)

    wall_start = args.epoch

    def sim_time():
        return (time.time() - wall_start) * args.speed

    start_sim = sim_time()
    if start_sim < -1.0 or start_sim > 3600.0:
        raise SystemExit(
            f"[ERROR] --epoch {args.epoch} looks stale: sim-time is already "
            f"{start_sim:.1f}s at startup. Use the CURRENT --epoch value that "
            f"producer.py printed at launch, identical across all three processes."
        )

    # entity -> feature -> deque of (event_ts, amount)
    buffers = defaultdict(lambda: defaultdict(deque))
    last_compute = defaultdict(lambda: defaultdict(lambda: -1.0))

    def maybe_recompute(entity, feature):
        """
        Time-triggered, NOT event-triggered: called on every loop tick for
        every known (entity, feature) pair, regardless of whether a new
        message just arrived. This is what makes it mirror
        sim_core.LaggedFeatureStore, which recomputes based on elapsed
        time whenever queried -- not only when new events land. Without
        this, a feature that goes quiet (low rate, or just past a burst)
        has its watermark freeze forever, permanently exceeding any
        decision_window -- a real bug the first version of this script had.
        """
        params = FEATURE_PARAMS[feature]
        now = sim_time()
        if now - last_compute[entity][feature] >= params["aggregation_window"]:
            watermark = now - params["ingestion_lag"]
            lo = watermark - params["window"]
            dq = buffers[entity][feature]
            while dq and dq[0] < lo:
                dq.popleft()
            value = sum(1 for e in dq if lo <= e <= watermark)

            key = f"feature:{entity}:{feature}"
            r.set(key, json.dumps({
                "value": value, "watermark": watermark, "computed_at": now,
            }))
            last_compute[entity][feature] = now

    print("Feature consumer running (poll-based, decoupled recompute). Ctrl-C to stop.")
    known_pairs = set()
    while True:
        records = consumer.poll(timeout_ms=100)
        for tp, messages in records.items():
            for msg in messages:
                ev = msg.value
                entity, feature, ts = ev["entity_id"], ev["feature_name"], ev["event_ts"]
                buffers[entity][feature].append(ts)
                known_pairs.add((entity, feature))

        # Recompute-due check runs every tick for every known pair, not
        # just the one that happened to receive a message.
        for entity, feature in known_pairs:
            maybe_recompute(entity, feature)


if __name__ == "__main__":
    main()
