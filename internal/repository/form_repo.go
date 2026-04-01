package repository

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/jmoiron/sqlx"
)

type FormRepo struct{ db *sqlx.DB }

func NewFormRepo(db *sqlx.DB) *FormRepo { return &FormRepo{db: db} }

func (r *FormRepo) Upsert(form models.FormRegistry) error {
	_, err := r.db.Exec(`
	INSERT INTO form_registry(schema_code, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode, is_enabled)
	VALUES($1,$2,$3,$4,$5,$6,$7)
	ON CONFLICT (schema_code) DO UPDATE
	SET display_name=EXCLUDED.display_name,
		chinese_remark=EXCLUDED.chinese_remark,
		sync_method=EXCLUDED.sync_method,
		sync_interval_minutes=EXCLUDED.sync_interval_minutes,
		sync_mode=EXCLUDED.sync_mode,
		is_enabled=EXCLUDED.is_enabled,
		updated_at=now();
	`, form.SchemaCode, form.DisplayName, form.ChineseRemark, form.SyncMethod, form.SyncIntervalMinutes, form.SyncMode, form.IsEnabled)
	return err
}

func (r *FormRepo) ListAllForms() ([]models.FormRegistry, error) {
	var rows []models.FormRegistry
	err := r.db.Select(&rows, `
	SELECT id, schema_code, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode,
	       last_sync_at, last_cursor_modified_time, last_cursor_object_id, is_enabled
	FROM form_registry
	ORDER BY updated_at DESC, id DESC;
	`)
	return rows, err
}

func (r *FormRepo) ListEnabledAutoDue(now time.Time) ([]models.FormRegistry, error) {
	var rows []models.FormRegistry
	err := r.db.Select(&rows, `
	SELECT id, schema_code, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode,
	       last_sync_at, last_cursor_modified_time, last_cursor_object_id, is_enabled
	FROM form_registry
	WHERE is_enabled=true
	  AND sync_method='AUTO'
	  AND (last_sync_at IS NULL OR last_sync_at + (sync_interval_minutes || ' minutes')::interval <= $1)
	ORDER BY id;
	`, now)
	return rows, err
}

func (r *FormRepo) ListRecentSyncLogs(limit int) ([]models.SyncLogView, error) {
	if limit <= 0 {
		limit = 20
	}
	var rows []models.SyncLogView
	err := r.db.Select(&rows, `
	SELECT l.id, f.schema_code, f.display_name, l.trigger_type, l.status, l.started_at, l.finished_at, l.synced_count, l.error_message
	FROM sync_logs l
	JOIN form_registry f ON f.id = l.form_id
	ORDER BY l.id DESC
	LIMIT $1;
	`, limit)
	return rows, err
}

func (r *FormRepo) ListBizColumns(schemaCode string) ([]string, error) {
	tbl := BizTableName(schemaCode)
	var cols []string
	err := r.db.Select(&cols, `
	SELECT column_name
	FROM information_schema.columns
	WHERE table_schema='public'
	  AND table_name=$1
	  AND column_name NOT IN ('object_id','modified_time','raw_json','created_at','updated_at')
	ORDER BY ordinal_position
	`, tbl)
	return cols, err
}

func (r *FormRepo) CountBizRowsForAdmin(schemaCode string, keyword string) (int, error) {
	tbl := BizTableName(schemaCode)
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)
	args := []interface{}{}
	if strings.TrimSpace(keyword) != "" {
		q += " WHERE raw_json::text ILIKE $1"
		args = append(args, "%"+keyword+"%")
	}
	var total int
	if err := r.db.Get(&total, q, args...); err != nil {
		return 0, err
	}
	return total, nil
}

func (r *FormRepo) ListBizRowsForAdmin(schemaCode string, columns []string, keyword string, limit int, offset int) ([]map[string]interface{}, error) {
	tbl := BizTableName(schemaCode)
	selectCols := []string{"object_id", "modified_time"}
	for _, c := range columns {
		if isSafeIdentifier(c) {
			selectCols = append(selectCols, c)
		}
	}

	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectCols, ","), tbl)
	args := []interface{}{}
	if strings.TrimSpace(keyword) != "" {
		q += " WHERE raw_json::text ILIKE $1"
		args = append(args, "%"+keyword+"%")
	}
	q += " ORDER BY modified_time DESC NULLS LAST, object_id DESC"
	q += " LIMIT $" + fmt.Sprintf("%d", len(args)+1)
	q += " OFFSET $" + fmt.Sprintf("%d", len(args)+2)
	args = append(args, limit, offset)

	rows, err := r.db.Queryx(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		m := map[string]interface{}{}
		if err := rows.MapScan(m); err != nil {
			return nil, err
		}
		result = append(result, normalizeMapScan(m))
	}
	return result, nil
}

func (r *FormRepo) GetBySchema(schema string) (models.FormRegistry, error) {
	var row models.FormRegistry
	err := r.db.Get(&row, `
	SELECT id, schema_code, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode,
	       last_sync_at, last_cursor_modified_time, last_cursor_object_id, is_enabled
	FROM form_registry WHERE schema_code=$1
	`, schema)
	return row, err
}

func (r *FormRepo) UpdateCursor(formID int64, modified *time.Time, objectID *string) error {
	_, err := r.db.Exec(`
	UPDATE form_registry
	SET last_sync_at=now(), last_cursor_modified_time=$2, last_cursor_object_id=$3, updated_at=now()
	WHERE id=$1
	`, formID, modified, objectID)
	return err
}

func (r *FormRepo) InsertSyncLog(formID int64, triggerType string) (int64, error) {
	var id int64
	err := r.db.QueryRow(`INSERT INTO sync_logs(form_id, trigger_type, status) VALUES($1,$2,'RUNNING') RETURNING id`, formID, triggerType).Scan(&id)
	return id, err
}

func (r *FormRepo) FinishSyncLog(logID int64, status string, count int, errMsg *string, cursorBefore *string, cursorAfter *string) error {
	_, err := r.db.Exec(`
	UPDATE sync_logs
	SET status=$2, synced_count=$3, error_message=$4, cursor_before=$5, cursor_after=$6, finished_at=now()
	WHERE id=$1
	`, logID, status, count, errMsg, cursorBefore, cursorAfter)
	return err
}

func (r *FormRepo) EnsureBizTable(schemaCode string, columns []string) error {
	tbl := BizTableName(schemaCode)
	baseSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		object_id text PRIMARY KEY,
		modified_time timestamptz NULL,
		raw_json jsonb NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now()
	);
	CREATE INDEX IF NOT EXISTS idx_%s_modified_time ON %s(modified_time);
	`, tbl, safeIndexName(tbl), tbl)
	if _, err := r.db.Exec(baseSQL); err != nil {
		return err
	}

	for _, c := range columns {
		if !isSafeIdentifier(c) {
			continue
		}
		stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s text`, tbl, c)
		if _, err := r.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (r *FormRepo) UpsertBizRow(schemaCode string, objectID string, modifiedTime *time.Time, rawJSON string, fields map[string]string) error {
	tbl := BizTableName(schemaCode)
	cols := []string{"object_id", "modified_time", "raw_json", "updated_at"}
	vals := []interface{}{objectID, modifiedTime, rawJSON, time.Now().UTC()}
	sets := []string{"modified_time=EXCLUDED.modified_time", "raw_json=EXCLUDED.raw_json", "updated_at=EXCLUDED.updated_at"}
	for k, v := range fields {
		if !isSafeIdentifier(k) {
			continue
		}
		cols = append(cols, k)
		vals = append(vals, v)
		sets = append(sets, fmt.Sprintf("%s=EXCLUDED.%s", k, k))
	}

	placeholders := make([]string, 0, len(cols))
	for idx := range cols {
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx+1))
	}
	query := fmt.Sprintf(`
	INSERT INTO %s(%s)
	VALUES(%s)
	ON CONFLICT (object_id)
	DO UPDATE SET %s
	`, tbl, strings.Join(cols, ","), strings.Join(placeholders, ","), strings.Join(sets, ","))
	_, err := r.db.Exec(query, vals...)
	return err
}

func (r *FormRepo) QueryRows(schemaCode string, whereSQL string, args []interface{}, limit int, offset int) ([]map[string]interface{}, error) {
	tbl := BizTableName(schemaCode)
	q := fmt.Sprintf("SELECT object_id, modified_time, raw_json FROM %s", tbl)
	if whereSQL != "" {
		q += " WHERE " + whereSQL
	}
	q += " ORDER BY modified_time DESC NULLS LAST, object_id DESC LIMIT $" + fmt.Sprintf("%d", len(args)+1) + " OFFSET $" + fmt.Sprintf("%d", len(args)+2)
	args = append(args, limit, offset)

	rows, err := r.db.Queryx(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		m := map[string]interface{}{}
		if err := rows.MapScan(m); err != nil {
			return nil, err
		}
		result = append(result, normalizeMapScan(m))
	}
	return result, nil
}

func (r *FormRepo) AddAPIQueryLog(log models.APIQueryLog) error {
	_, err := r.db.Exec(`
	INSERT INTO api_query_logs(api_key_id, schema_code, query_payload, result_count, duration_ms, caller_ip)
	VALUES($1,$2,$3,$4,$5,$6)
	`, log.APIKeyID, log.SchemaCode, log.QueryPayload, log.ResultCount, log.DurationMS, log.CallerIP)
	return err
}

func (r *FormRepo) UpsertFieldRemark(formID int64, field models.FormFieldRegistry) error {
	_, err := r.db.Exec(`
	INSERT INTO form_field_registry(form_id, field_code, field_name, chinese_remark, show_in_admin, original_type, storage_type)
	VALUES($1,$2,$3,$4,$5,$6,$7)
	ON CONFLICT (form_id, field_code) DO UPDATE
	SET field_name=EXCLUDED.field_name,
		chinese_remark=EXCLUDED.chinese_remark,
		show_in_admin=EXCLUDED.show_in_admin,
		original_type=EXCLUDED.original_type,
		storage_type=EXCLUDED.storage_type,
		updated_at=now();
	`, formID, field.FieldCode, field.FieldName, field.ChineseRemark, field.ShowInAdmin, field.OriginalType, field.StorageType)
	return err
}

func (r *FormRepo) ListFieldRemarks(formID int64) ([]models.FormFieldRegistry, error) {
	var rows []models.FormFieldRegistry
	err := r.db.Select(&rows, `
	SELECT id, form_id, field_code, field_name, chinese_remark, show_in_admin, original_type, storage_type
	FROM form_field_registry WHERE form_id=$1 ORDER BY id
	`, formID)
	return rows, err
}

func (r *FormRepo) ValidateAPIKeyAndForm(keyHash string, schemaCode string) (models.APIKey, error) {
	var k models.APIKey
	err := r.db.Get(&k, `
	SELECT ak.id, ak.name, ak.remark, ak.key_hash, ak.key_prefix, ak.expires_at, ak.revoked_at, ak.created_at
	FROM api_keys ak
	JOIN api_key_form_permissions p ON p.api_key_id=ak.id
	JOIN form_registry fr ON fr.id=p.form_id
	WHERE ak.key_hash=$1
	  AND fr.schema_code=$2
	  AND ak.revoked_at IS NULL
	  AND (ak.expires_at IS NULL OR ak.expires_at > now())
	LIMIT 1
	`, keyHash, schemaCode)
	return k, err
}

func (r *FormRepo) CreateAPIKey(name, remark, keyHash, keyPrefix string, expiresAt *time.Time, schemaCodes []string) (int64, error) {
	tx, err := r.db.Beginx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var id int64
	if err := tx.QueryRow(`
	INSERT INTO api_keys(name, remark, key_hash, key_prefix, expires_at)
	VALUES($1,$2,$3,$4,$5)
	RETURNING id
	`, name, remark, keyHash, keyPrefix, expiresAt).Scan(&id); err != nil {
		return 0, err
	}

	for _, code := range schemaCodes {
		if _, err := tx.Exec(`
		INSERT INTO api_key_form_permissions(api_key_id, form_id)
		SELECT $1, id FROM form_registry WHERE schema_code=$2
		`, id, code); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

func normalizeMapScan(m map[string]interface{}) map[string]interface{} {
	res := map[string]interface{}{}
	for k, v := range m {
		switch tv := v.(type) {
		case []byte:
			res[k] = string(tv)
		default:
			res[k] = tv
		}
	}
	return res
}

func BizTableName(schemaCode string) string {
	return `biz_` + strings.ToLower(schemaCode)
}

func isSafeIdentifier(v string) bool {
	if v == "" {
		return false
	}
	for _, ch := range v {
		if !(ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
			return false
		}
	}
	return true
}

func safeIndexName(tbl string) string {
	tbl = strings.ReplaceAll(tbl, ".", "_")
	if len(tbl) > 45 {
		return tbl[:45]
	}
	return tbl
}

func NullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}
