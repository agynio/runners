ALTER TABLE runners
ADD COLUMN ziti_identity_id TEXT NOT NULL DEFAULT '',
ADD COLUMN ziti_service_id TEXT NOT NULL DEFAULT '',
ADD COLUMN ziti_service_name TEXT NOT NULL DEFAULT '';
