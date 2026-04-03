ALTER TABLE enterprise_library_forms
    ADD COLUMN IF NOT EXISTS sheet_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_checksum TEXT NOT NULL DEFAULT '';

UPDATE enterprise_library_forms
SET source_checksum = file_checksum
WHERE COALESCE(source_checksum, '') = '';

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    SELECT tc.constraint_name
    INTO constraint_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
     AND tc.table_schema = ccu.table_schema
    WHERE tc.table_schema = 'public'
      AND tc.table_name = 'enterprise_library_forms'
      AND tc.constraint_type = 'UNIQUE'
      AND ccu.column_name = 'file_checksum'
    LIMIT 1;

    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE enterprise_library_forms DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_enterprise_library_forms_source_sheet
ON enterprise_library_forms(source_checksum, sheet_name);

CREATE INDEX IF NOT EXISTS idx_enterprise_library_forms_source_checksum
ON enterprise_library_forms(source_checksum);
