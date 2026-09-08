-- Terminal records written before removed_at was enforced: a runner-reported
-- failure landed as failed with no removal time. Nothing writes to a terminal
-- row again, so the write-time invariant never reaches them, and metering
-- treats a row without removed_at as still alive. updated_at is the moment
-- the terminal status was recorded, the closest honest removal time.
UPDATE workloads
SET removed_at = updated_at
WHERE status IN ('failed', 'stopped')
  AND removed_at IS NULL;

UPDATE volumes
SET removed_at = updated_at
WHERE status IN ('deleted', 'failed')
  AND removed_at IS NULL;
