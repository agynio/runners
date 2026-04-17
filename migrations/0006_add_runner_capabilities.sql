ALTER TABLE runners
ADD COLUMN capabilities JSONB NOT NULL DEFAULT '[]'::jsonb;
