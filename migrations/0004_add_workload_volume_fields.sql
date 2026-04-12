ALTER TABLE workloads
ADD COLUMN IF NOT EXISTS instance_id TEXT,
ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS last_metering_sampled_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS volumes (
    id UUID PRIMARY KEY,
    instance_id TEXT,
    volume_id UUID NOT NULL,
    thread_id UUID NOT NULL,
    runner_id UUID NOT NULL REFERENCES runners(id) ON DELETE CASCADE,
    agent_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    size_gb TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'provisioning'
                   CHECK (status IN ('provisioning', 'active', 'deprovisioning', 'deleted', 'failed')),
    removed_at TIMESTAMPTZ,
    last_metering_sampled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_volumes_runner_id ON volumes (runner_id);
CREATE INDEX IF NOT EXISTS idx_volumes_thread_id ON volumes (thread_id);
CREATE INDEX IF NOT EXISTS idx_volumes_agent_id ON volumes (agent_id);
CREATE INDEX IF NOT EXISTS idx_volumes_organization_id ON volumes (organization_id);
CREATE INDEX IF NOT EXISTS idx_volumes_status ON volumes (status);
