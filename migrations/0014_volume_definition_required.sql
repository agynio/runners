-- A volume record without a definition behind it was the implicit sandbox
-- workspace disk: the Orchestrator provisioned 10Gi at /workspace for every
-- sandbox, described by nothing an operator had declared. A sandbox now mounts
-- what its environment declares and nothing else, so every record names the
-- definition it was made from.
--
-- Those rows are deleted rather than translated. There is no definition to point
-- them at -- that was the whole problem with them -- and the PVCs they describe
-- are reclaimed by volume reconciliation, which removes a disk present on a
-- runner that no record claims.
DELETE FROM volumes WHERE volume_id IS NULL;

ALTER TABLE volumes
    ALTER COLUMN volume_id SET NOT NULL;

-- The old shape check required the definition only for an agent-instance owner,
-- which is what let a sandbox record carry none. Ownership still differs -- an
-- agent instance carries a thread and a class, a sandbox carries neither -- but
-- the definition is now common to both.
ALTER TABLE volumes
    DROP CONSTRAINT IF EXISTS volumes_agent_instance_shape_check;

ALTER TABLE volumes
    ADD CONSTRAINT volumes_agent_instance_shape_check CHECK (
        owner_kind <> 'agent_instance'
        OR (thread_id IS NOT NULL AND agent_id IS NOT NULL)
    );
