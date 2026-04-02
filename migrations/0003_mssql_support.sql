ALTER TABLE form_registry
ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'H3';

CREATE TABLE IF NOT EXISTS system_settings (
    key_name TEXT PRIMARY KEY,
    value_text TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mssql_form_registry (
    form_id BIGINT PRIMARY KEY REFERENCES form_registry(id) ON DELETE CASCADE,
    source_schema TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_full_name TEXT NOT NULL UNIQUE,
    incremental_column TEXT NOT NULL DEFAULT 'id',
    last_processed_file TEXT NULL,
    last_processed_at TIMESTAMPTZ NULL,
    last_scanned_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mssql_processed_files (
    id BIGSERIAL PRIMARY KEY,
    form_id BIGINT NOT NULL REFERENCES form_registry(id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_mode TEXT NOT NULL,
    file_time TIMESTAMPTZ NULL,
    row_count INT NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(form_id, file_path)
);

CREATE INDEX IF NOT EXISTS idx_form_registry_source_type ON form_registry(source_type);
CREATE INDEX IF NOT EXISTS idx_mssql_processed_files_form_time ON mssql_processed_files(form_id, file_time DESC);
