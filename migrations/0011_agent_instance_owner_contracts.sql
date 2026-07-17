CREATE INDEX IF NOT EXISTS idx_workloads_agent_instance_created_at
    ON workloads (owner_kind, owner_id, created_at DESC, id DESC)
    WHERE owner_kind = 'agent_instance';

CREATE INDEX IF NOT EXISTS idx_volumes_agent_instance_id
    ON volumes (owner_kind, owner_id, id)
    WHERE owner_kind = 'agent_instance';
