package models

import "time"

type SyncLog struct {
	ID           int64      `db:"id"`
	FormID       int64      `db:"form_id"`
	TriggerType  string     `db:"trigger_type"`
	Status       string     `db:"status"`
	StartedAt    time.Time  `db:"started_at"`
	FinishedAt   *time.Time `db:"finished_at"`
	SyncedCount  int        `db:"synced_count"`
	ErrorMessage *string    `db:"error_message"`
	CursorBefore *string    `db:"cursor_before"`
	CursorAfter  *string    `db:"cursor_after"`
}

type APIQueryLog struct {
	ID           int64     `db:"id"`
	APIKeyID     int64     `db:"api_key_id"`
	SchemaCode   string    `db:"schema_code"`
	QueryPayload string    `db:"query_payload"`
	ResultCount  int       `db:"result_count"`
	DurationMS   int       `db:"duration_ms"`
	CallerIP     string    `db:"caller_ip"`
	CreatedAt    time.Time `db:"created_at"`
}

type AdminAuditLog struct {
	ID         int64     `db:"id"`
	Username   string    `db:"username"`
	Action     string    `db:"action"`
	TargetType string    `db:"target_type"`
	TargetID   string    `db:"target_id"`
	Detail     string    `db:"detail"`
	ClientIP   string    `db:"client_ip"`
	CreatedAt  time.Time `db:"created_at"`
}

type SyncLogView struct {
	ID           int64      `db:"id"`
	SchemaCode   string     `db:"schema_code"`
	DisplayName  string     `db:"display_name"`
	TriggerType  string     `db:"trigger_type"`
	Status       string     `db:"status"`
	StartedAt    time.Time  `db:"started_at"`
	FinishedAt   *time.Time `db:"finished_at"`
	SyncedCount  int        `db:"synced_count"`
	ErrorMessage *string    `db:"error_message"`
}
