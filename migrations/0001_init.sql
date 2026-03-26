CREATE TABLE IF NOT EXISTS schema_migrations (
    version  TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE runners (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL,
    organization_id    UUID,
    identity_id        UUID NOT NULL UNIQUE,
    service_token_hash TEXT NOT NULL UNIQUE,
    status             TEXT NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'enrolled', 'offline')),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_runners_organization_id ON runners (organization_id);
CREATE INDEX idx_runners_status ON runners (status);

CREATE TABLE workloads (
    id               UUID PRIMARY KEY,
    runner_id        UUID NOT NULL REFERENCES runners(id) ON DELETE CASCADE,
    thread_id        UUID NOT NULL,
    agent_id         UUID NOT NULL,
    organization_id  UUID NOT NULL,
    status           TEXT NOT NULL DEFAULT 'starting'
                               CHECK (status IN ('starting', 'running', 'stopping', 'stopped', 'failed')),
    containers       JSONB NOT NULL DEFAULT '[]'::jsonb,
    ziti_identity_id TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_workloads_runner_id ON workloads (runner_id);
CREATE INDEX idx_workloads_thread_id ON workloads (thread_id);
CREATE INDEX idx_workloads_agent_id ON workloads (agent_id);
CREATE INDEX idx_workloads_status ON workloads (status);
