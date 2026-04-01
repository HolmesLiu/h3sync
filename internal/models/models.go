package models

import "time"

type FormRegistry struct {
	ID                     int64      `db:"id"`
	SchemaCode             string     `db:"schema_code"`
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

type FormFieldRegistry struct {
	ID             int64  `db:"id"`
	FormID         int64  `db:"form_id"`
	FieldCode      string `db:"field_code"`
	FieldName      string `db:"field_name"`
	ChineseRemark  string `db:"chinese_remark"`
	ShowInAdmin    bool   `db:"show_in_admin"`
	OriginalType   string `db:"original_type"`
	StorageType    string `db:"storage_type"`
}

type APIKey struct {
	ID          int64      `db:"id"`
	Name        string     `db:"name"`
	Remark      string     `db:"remark"`
	KeyHash     string     `db:"key_hash"`
	KeyPrefix   string     `db:"key_prefix"`
	ExpiresAt   *time.Time `db:"expires_at"`
	RevokedAt   *time.Time `db:"revoked_at"`
	CreatedAt   time.Time  `db:"created_at"`
}
