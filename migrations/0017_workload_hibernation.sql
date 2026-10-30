-- A workload that is neither running nor stopped: paused and persisted by its
-- runner, occupying storage instead of compute, back in under a second.
--
-- hibernated_at is when the workload most recently entered hibernated. It
-- bounds the compute interval metering bills and is what hibernation_ttl is
-- measured from. snapshot_size_bytes is the size of the persisted state the
-- runner reported with hibernated -- what a sleeping workload is metered on
-- instead of its flavor. Both are NULL while the workload is awake. woken_at
-- is when it most recently left hibernated for running, NULL until its first
-- wake, and kept after it.
ALTER TABLE workloads
ADD COLUMN IF NOT EXISTS hibernated_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS woken_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS snapshot_size_bytes BIGINT;

-- The status vocabulary gains the three states around a sleep. The check from
-- the initial migration was declared inline on the column, so it carries the
-- name PostgreSQL gives such a constraint.
ALTER TABLE workloads
    DROP CONSTRAINT IF EXISTS workloads_status_check;

ALTER TABLE workloads
    ADD CONSTRAINT workloads_status_check CHECK (
        status IN ('starting', 'running', 'hibernating', 'hibernated', 'waking', 'stopping', 'stopped', 'failed')
    );
