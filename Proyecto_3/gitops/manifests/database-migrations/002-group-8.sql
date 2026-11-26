\set ON_ERROR_STOP on

BEGIN;

SET ROLE proyecto3_owner;

ALTER TABLE raw_data.batches
ALTER COLUMN source_group SET DEFAULT 8;

RESET ROLE;

COMMIT;

SELECT column_name, column_default
FROM information_schema.columns
WHERE table_schema = 'raw_data'
  AND table_name = 'batches'
  AND column_name = 'source_group';
