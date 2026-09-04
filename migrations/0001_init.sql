CREATE TABLE devices (
    id              UUID PRIMARY KEY,
    lat             DOUBLE PRECISION NOT NULL,
    lon             DOUBLE PRECISION NOT NULL,
    geohash         VARCHAR(12) NOT NULL,
    shard_prefix    VARCHAR(8) NOT NULL,
    registered_at   TIMESTAMPTZ NOT NULL,
    last_seen       TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_devices_shard_prefix ON devices (shard_prefix);
CREATE INDEX idx_devices_geohash ON devices (geohash);

CREATE TABLE alerts (
    id              UUID PRIMARY KEY,
    center_lat      DOUBLE PRECISION NOT NULL,
    center_lon      DOUBLE PRECISION NOT NULL,
    radius_meters   DOUBLE PRECISION NOT NULL,
    severity        VARCHAR(16) NOT NULL,
    title           TEXT NOT NULL,
    message         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);

-- One row per shard prefix an alert was actually published to. This is
-- the audit trail the wow-moment demo reads from: "this alert hit 6
-- shards covering ~142K devices in 340ms" comes from aggregating this
-- table, not from trusting the coordinator's in-memory state.
CREATE TABLE alert_deliveries (
    id                  UUID PRIMARY KEY,
    alert_id            UUID NOT NULL REFERENCES alerts(id),
    shard_prefix        VARCHAR(8) NOT NULL,
    published_at        TIMESTAMPTZ NOT NULL,
    devices_in_shard    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_alert_deliveries_alert_id ON alert_deliveries (alert_id);