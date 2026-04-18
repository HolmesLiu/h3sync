ALTER TABLE form_registry
ADD COLUMN IF NOT EXISTS sync_schedule_type TEXT NOT NULL DEFAULT 'DAILY';

ALTER TABLE form_registry
ADD COLUMN IF NOT EXISTS sync_schedule_weekday INT NULL;

ALTER TABLE form_registry
ADD COLUMN IF NOT EXISTS sync_schedule_hour INT NOT NULL DEFAULT 2;

ALTER TABLE form_registry
ADD COLUMN IF NOT EXISTS sync_schedule_minute INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_sync_logs_status_started_at ON sync_logs(status, started_at);
