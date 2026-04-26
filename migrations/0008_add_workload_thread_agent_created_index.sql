-- Supports ListWorkloadsByThread cursor ordering.
CREATE INDEX IF NOT EXISTS idx_workloads_thread_agent_created_at
    ON workloads (thread_id, agent_id, created_at DESC, id DESC);
