-- The runner's reported catalog. Entries are declared in the runner's own
-- configuration and replaced wholesale on every report, so they carry no
-- platform id and nothing references them by one: environments and volumes
-- name them, and the name is resolved at workload start.
CREATE TABLE IF NOT EXISTS runner_flavors (
    runner_id       UUID    NOT NULL REFERENCES runners (id) ON DELETE CASCADE,
    name            TEXT    NOT NULL,
    requests_cpu    TEXT    NOT NULL,
    requests_memory TEXT    NOT NULL,
    limits_cpu      TEXT    NOT NULL,
    limits_memory   TEXT    NOT NULL,
    is_default      BOOLEAN NOT NULL DEFAULT FALSE,
    deprecated      BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (runner_id, name)
);

CREATE TABLE IF NOT EXISTS runner_storage_classes (
    runner_id  UUID    NOT NULL REFERENCES runners (id) ON DELETE CASCADE,
    name       TEXT    NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    deprecated BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (runner_id, name)
);

-- At most one default per runner per kind. A partial unique index expresses
-- this without forbidding many non-default entries.
CREATE UNIQUE INDEX IF NOT EXISTS idx_runner_flavors_single_default
    ON runner_flavors (runner_id)
    WHERE is_default;

CREATE UNIQUE INDEX IF NOT EXISTS idx_runner_storage_classes_single_default
    ON runner_storage_classes (runner_id)
    WHERE is_default;
