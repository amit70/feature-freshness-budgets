CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    event_ts DOUBLE PRECISION NOT NULL,  -- simulation-relative seconds, matches producer's clock
    amount DOUBLE PRECISION NOT NULL
);

CREATE INDEX idx_events_entity_feature_ts ON events (entity_id, feature_name, event_ts);
