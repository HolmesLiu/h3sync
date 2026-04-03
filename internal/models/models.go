package models

import "time"

type FormRegistry struct {
	ID                     int64      `db:"id"`
	SchemaCode             string     `db:"schema_code"`
	SourceType             string     `db:"source_type"`
	GroupName              string     `db:"group_name"`
	DisplayName            string     `db:"display_name"`
	ChineseRemark          string     `db:"chinese_remark"`
	SyncMethod             string     `db:"sync_method"`
	SyncIntervalMinutes    int        `db:"sync_interval_minutes"`
	SyncMode               string     `db:"sync_mode"`
	LastSyncAt             *time.Time `db:"last_sync_at"`
	LastCursorModifiedTime *time.Time `db:"last_cursor_modified_time"`
	LastCursorObjectID     *string    `db:"last_cursor_object_id"`
	IsEnabled              bool       `db:"is_enabled"`
}

type SystemSetting struct {
	Key   string `db:"key_name"`
	Value string `db:"value_text"`
}

type MSSQLFormRegistry struct {
	FormID            int64      `db:"form_id"`
	SourceSchema      string     `db:"source_schema"`
	SourceTable       string     `db:"source_table"`
	SourceFullName    string     `db:"source_full_name"`
	IncrementalColumn string     `db:"incremental_column"`
	LastProcessedFile *string    `db:"last_processed_file"`
	LastProcessedAt   *time.Time `db:"last_processed_at"`
	LastScannedAt     *time.Time `db:"last_scanned_at"`
}

type MSSQLFormListView struct {
	FormID              int64      `db:"form_id"`
	SchemaCode          string     `db:"schema_code"`
	GroupName           string     `db:"group_name"`
	DisplayName         string     `db:"display_name"`
	ChineseRemark       string     `db:"chinese_remark"`
	SyncMethod          string     `db:"sync_method"`
	SyncIntervalMinutes int        `db:"sync_interval_minutes"`
	LastSyncAt          *time.Time `db:"last_sync_at"`
	SourceSchema        string     `db:"source_schema"`
	SourceTable         string     `db:"source_table"`
	SourceFullName      string     `db:"source_full_name"`
	IncrementalColumn   string     `db:"incremental_column"`
	LastProcessedFile   *string    `db:"last_processed_file"`
	LastProcessedAt     *time.Time `db:"last_processed_at"`
	LastScannedAt       *time.Time `db:"last_scanned_at"`
}

type FormFieldRegistry struct {
	ID            int64  `db:"id"`
	FormID        int64  `db:"form_id"`
	FieldCode     string `db:"field_code"`
	FieldName     string `db:"field_name"`
	ChineseRemark string `db:"chinese_remark"`
	ShowInAdmin   bool   `db:"show_in_admin"`
	OriginalType  string `db:"original_type"`
	StorageType   string `db:"storage_type"`
}

type APIKey struct {
	ID        int64      `db:"id"`
	Name      string     `db:"name"`
	Remark    string     `db:"remark"`
	KeyHash   string     `db:"key_hash"`
	KeyPrefix string     `db:"key_prefix"`
	KeyValue  string     `db:"key_value"`
	ExpiresAt *time.Time `db:"expires_at"`
	RevokedAt *time.Time `db:"revoked_at"`
	CreatedAt time.Time  `db:"created_at"`
}

type APIKeyListView struct {
	ID           int64      `db:"id"`
	Name         string     `db:"name"`
	Remark       string     `db:"remark"`
	KeyPrefix    string     `db:"key_prefix"`
	KeyValue     string     `db:"key_value"`
	ExpiresAt    *time.Time `db:"expires_at"`
	RevokedAt    *time.Time `db:"revoked_at"`
	CreatedAt    time.Time  `db:"created_at"`
	AllowedForms string     `db:"allowed_forms"`
}

type AdminUser struct {
	ID           int64     `db:"id"`
	Username     string    `db:"username"`
	PasswordHash string    `db:"password_hash"`
	IsActive     bool      `db:"is_active"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

type AgentRole struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
}
