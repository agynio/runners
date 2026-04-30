ALTER TABLE workloads
ADD COLUMN IF NOT EXISTS agent_state TEXT NOT NULL DEFAULT 'processing'
    CHECK (agent_state IN ('processing', 'idle'));
