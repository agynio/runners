ALTER TABLE workloads
ADD COLUMN IF NOT EXISTS agent_state TEXT NOT NULL DEFAULT 'processing'
    CHECK (agent_state IN ('processing', 'idle'));

CREATE INDEX IF NOT EXISTS idx_workloads_agent_state ON workloads (agent_state);

-- Supports the Agent Activity Sweep query.
CREATE INDEX IF NOT EXISTS idx_workloads_agent_activity_sweep
    ON workloads (last_activity_at)
    WHERE status = 'running' AND agent_state = 'processing' AND removed_at IS NULL;
