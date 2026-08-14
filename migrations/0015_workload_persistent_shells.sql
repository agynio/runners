-- Whether shells in this workload outlive the connections that reach them.
--
-- Resolved from the environment by the Orchestrator at start and fixed for the
-- workload's life, so the Terminal Proxy can answer the question from the
-- GetWorkload it already makes -- and so editing an environment does not
-- reconfigure a workload someone is working in.
--
-- Defaults true for rows that predate the column, matching the environment
-- default.
ALTER TABLE workloads
ADD COLUMN IF NOT EXISTS persistent_shells BOOLEAN NOT NULL DEFAULT TRUE;
