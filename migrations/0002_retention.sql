-- Phase 11: data lifecycle and a radius-aware audit trail.

-- 1. Deleting an alert (retention sweep) must also delete its delivery rows.
ALTER TABLE alert_deliveries
    DROP CONSTRAINT IF EXISTS alert_deliveries_alert_id_fkey;
ALTER TABLE alert_deliveries
    ADD CONSTRAINT alert_deliveries_alert_id_fkey
    FOREIGN KEY (alert_id) REFERENCES alerts (id) ON DELETE CASCADE;

-- 2. devices_in_shard counts the whole ~39km x 19.5km cell. This column
--    counts only the devices that are inside the alert circle.
ALTER TABLE alert_deliveries
    ADD COLUMN IF NOT EXISTS devices_in_radius INTEGER NOT NULL DEFAULT 0;

-- 3. One delivery row per (alert, shard). Remove any accidental duplicates
--    first, then enforce it so recording a delivery can be idempotent.
DELETE FROM alert_deliveries a
USING alert_deliveries b
WHERE a.alert_id = b.alert_id
  AND a.shard_prefix = b.shard_prefix
  AND a.id > b.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_deliveries_alert_shard
    ON alert_deliveries (alert_id, shard_prefix);

-- 4. Indexes for the retention sweeps and the "recent alerts" listing.
CREATE INDEX IF NOT EXISTS idx_devices_last_seen ON devices (last_seen);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
