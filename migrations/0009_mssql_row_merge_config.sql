ALTER TABLE mssql_form_registry
ADD COLUMN IF NOT EXISTS stock_update_enabled BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE mssql_form_registry
ADD COLUMN IF NOT EXISTS unique_key_column TEXT NOT NULL DEFAULT '';
