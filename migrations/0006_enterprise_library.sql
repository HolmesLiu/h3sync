CREATE TABLE IF NOT EXISTS enterprise_library_forms (
    form_id BIGINT PRIMARY KEY REFERENCES form_registry(id) ON DELETE CASCADE,
    source_filename TEXT NOT NULL,
    stored_filename TEXT NOT NULL,
    file_ext TEXT NOT NULL DEFAULT '',
    mime_type TEXT NOT NULL DEFAULT '',
    file_size BIGINT NOT NULL DEFAULT 0,
    file_checksum TEXT NOT NULL,
    file_path TEXT NOT NULL,
    parse_status TEXT NOT NULL DEFAULT 'READY',
    parse_message TEXT NOT NULL DEFAULT '',
    row_count INT NOT NULL DEFAULT 0,
    parsed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(file_checksum)
);

CREATE INDEX IF NOT EXISTS idx_enterprise_library_forms_status ON enterprise_library_forms(parse_status);
