ALTER TABLE workloads
    ADD COLUMN IF NOT EXISTS owner_kind TEXT NOT NULL DEFAULT 'agent_instance',
    ADD COLUMN IF NOT EXISTS owner_id UUID;

UPDATE workloads
SET owner_id = thread_id
WHERE owner_id IS NULL;

ALTER TABLE workloads
    ALTER COLUMN owner_id SET NOT NULL,
    ALTER COLUMN agent_id DROP NOT NULL,
    ALTER COLUMN thread_id DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'workloads_owner_kind_check'
    ) THEN
        ALTER TABLE workloads
            ADD CONSTRAINT workloads_owner_kind_check
            CHECK (owner_kind IN ('agent_instance', 'sandbox'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'workloads_agent_instance_shape_check'
    ) THEN
        ALTER TABLE workloads
            ADD CONSTRAINT workloads_agent_instance_shape_check
            CHECK (
                owner_kind <> 'agent_instance'
                OR (thread_id IS NOT NULL AND agent_id IS NOT NULL)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_workloads_owner
    ON workloads (owner_kind, owner_id);

ALTER TABLE volumes
    ADD COLUMN IF NOT EXISTS owner_kind TEXT NOT NULL DEFAULT 'agent_instance',
    ADD COLUMN IF NOT EXISTS owner_id UUID;

UPDATE volumes
SET owner_id = thread_id
WHERE owner_id IS NULL;

ALTER TABLE volumes
    ALTER COLUMN owner_id SET NOT NULL,
    ALTER COLUMN volume_id DROP NOT NULL,
    ALTER COLUMN thread_id DROP NOT NULL,
    ALTER COLUMN agent_id DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'volumes_owner_kind_check'
    ) THEN
        ALTER TABLE volumes
            ADD CONSTRAINT volumes_owner_kind_check
            CHECK (owner_kind IN ('agent_instance', 'sandbox'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'volumes_agent_instance_shape_check'
    ) THEN
        ALTER TABLE volumes
            ADD CONSTRAINT volumes_agent_instance_shape_check
            CHECK (
                owner_kind <> 'agent_instance'
                OR (volume_id IS NOT NULL AND thread_id IS NOT NULL AND agent_id IS NOT NULL)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_volumes_owner
    ON volumes (owner_kind, owner_id);
