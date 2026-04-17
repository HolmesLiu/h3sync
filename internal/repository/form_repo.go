package repository

import (
	"database/sql"
	"encoding/json"
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
	INSERT INTO form_registry(schema_code, source_type, group_name, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode, is_enabled)
	VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
	ON CONFLICT (schema_code) DO UPDATE
	SET source_type=EXCLUDED.source_type,
		group_name=EXCLUDED.group_name,
		display_name=EXCLUDED.display_name,
		chinese_remark=EXCLUDED.chinese_remark,
		sync_method=EXCLUDED.sync_method,
		sync_interval_minutes=EXCLUDED.sync_interval_minutes,
		sync_mode=EXCLUDED.sync_mode,
		is_enabled=EXCLUDED.is_enabled,
		updated_at=now();
	`, form.SchemaCode, form.SourceType, form.GroupName, form.DisplayName, form.ChineseRemark, form.SyncMethod, form.SyncIntervalMinutes, form.SyncMode, form.IsEnabled)
	return err
}

func (r *FormRepo) DeleteBySchema(schema string) error {
	form, err := r.GetBySchema(schema)
	if err != nil {
		return err
	}
	tbl := BizTableName(schema)

	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM sync_logs WHERE form_id=$1`, form.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM api_key_form_permissions WHERE form_id=$1`, form.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM form_field_registry WHERE form_id=$1`, form.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM form_registry WHERE id=$1`, form.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tbl)); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *FormRepo) ListAllForms() ([]models.FormRegistry, error) {
	var rows []models.FormRegistry
	err := r.db.Select(&rows, `
	SELECT id, schema_code, source_type, group_name, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode,
	       last_sync_at, last_cursor_modified_time, last_cursor_object_id, is_enabled
	FROM form_registry
	ORDER BY updated_at DESC, id DESC;
	`)
	return rows, err
}

func (r *FormRepo) ListFormsBySource(sourceType string) ([]models.FormRegistry, error) {
	var rows []models.FormRegistry
	err := r.db.Select(&rows, `
	SELECT id, schema_code, source_type, group_name, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode,
	       last_sync_at, last_cursor_modified_time, last_cursor_object_id, is_enabled
	FROM form_registry
	WHERE source_type=$1
	ORDER BY updated_at DESC, id DESC;
	`, sourceType)
	return rows, err
}

func (r *FormRepo) ListGroupNames() ([]string, error) {
	var groups []string
	err := r.db.Select(&groups, `
	SELECT DISTINCT group_name
	FROM form_registry
	WHERE group_name IS NOT NULL AND btrim(group_name) <> ''
	ORDER BY group_name;
	`)
	return groups, err
}

func (r *FormRepo) ListGroupNamesBySource(sourceType string) ([]string, error) {
	var groups []string
	err := r.db.Select(&groups, `
	SELECT DISTINCT group_name
	FROM form_registry
	WHERE source_type=$1
	  AND group_name IS NOT NULL
	  AND btrim(group_name) <> ''
	ORDER BY group_name;
	`, sourceType)
	return groups, err
}

func (r *FormRepo) RenameGroup(oldName string, newName string) error {
	oldName = strings.TrimSpace(oldName)
	newName = strings.TrimSpace(newName)
	if oldName == "" || newName == "" {
		return fmt.Errorf("group name cannot be empty")
	}
	_, err := r.db.Exec(`
	UPDATE form_registry
	SET group_name=$2, updated_at=now()
	WHERE group_name=$1
	`, oldName, newName)
	return err
}

func (r *FormRepo) RenameGroupBySource(oldName string, newName string, sourceType string) error {
	oldName = strings.TrimSpace(oldName)
	newName = strings.TrimSpace(newName)
	if oldName == "" || newName == "" {
		return fmt.Errorf("group name cannot be empty")
	}
	_, err := r.db.Exec(`
	UPDATE form_registry
	SET group_name=$2, updated_at=now()
	WHERE group_name=$1 AND source_type=$3
	`, oldName, newName, sourceType)
	return err
}

func (r *FormRepo) ListEnabledAutoDue(now time.Time) ([]models.FormRegistry, error) {
	var rows []models.FormRegistry
	err := r.db.Select(&rows, `
	SELECT id, schema_code, source_type, group_name, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode,
	       last_sync_at, last_cursor_modified_time, last_cursor_object_id, is_enabled
	FROM form_registry
	WHERE is_enabled=true
	  AND sync_method='AUTO'
	  AND (last_sync_at IS NULL OR last_sync_at + (sync_interval_minutes || ' minutes')::interval <= $1)
	ORDER BY id;
	`, now)
	return rows, err
}

func (r *FormRepo) ListSyncLogs(limit int, offset int, status string, trigger string) ([]models.SyncLogView, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	where := []string{"1=1"}
	args := []interface{}{}
	argN := 1
	if strings.TrimSpace(status) != "" {
		where = append(where, fmt.Sprintf("l.status = $%d", argN))
		args = append(args, status)
		argN++
	}
	if strings.TrimSpace(trigger) != "" {
		where = append(where, fmt.Sprintf("l.trigger_type = $%d", argN))
		args = append(args, trigger)
		argN++
	}
	args = append(args, limit, offset)
	limitPos := argN
	offsetPos := argN + 1

	var rows []models.SyncLogView
	query := fmt.Sprintf(`
	SELECT l.id, f.schema_code, f.display_name, l.trigger_type, l.status, l.started_at, l.finished_at, l.synced_count, l.error_message
	FROM sync_logs l
	JOIN form_registry f ON f.id = l.form_id
	WHERE %s
	ORDER BY l.id DESC
	LIMIT $%d OFFSET $%d;
	`, strings.Join(where, " AND "), limitPos, offsetPos)

	err := r.db.Select(&rows, query, args...)
	return rows, err
}

func (r *FormRepo) CountSyncLogs(status string, trigger string) (int, error) {
	where := []string{"1=1"}
	args := []interface{}{}
	argN := 1
	if strings.TrimSpace(status) != "" {
		where = append(where, fmt.Sprintf("status = $%d", argN))
		args = append(args, status)
		argN++
	}
	if strings.TrimSpace(trigger) != "" {
		where = append(where, fmt.Sprintf("trigger_type = $%d", argN))
		args = append(args, trigger)
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM sync_logs WHERE %s", strings.Join(where, " AND "))
	var total int
	err := r.db.Get(&total, query, args...)
	return total, err
}

func (r *FormRepo) CountSyncLogsBySource(sourceType string, status string, trigger string) (int, error) {
	where := []string{"fr.source_type = $1"}
	args := []interface{}{sourceType}
	argN := 2
	if strings.TrimSpace(status) != "" {
		where = append(where, fmt.Sprintf("l.status = $%d", argN))
		args = append(args, status)
		argN++
	}
	if strings.TrimSpace(trigger) != "" {
		where = append(where, fmt.Sprintf("l.trigger_type = $%d", argN))
		args = append(args, trigger)
	}
	query := fmt.Sprintf(`
	SELECT COUNT(*)
	FROM sync_logs l
	JOIN form_registry fr ON fr.id = l.form_id
	WHERE %s
	`, strings.Join(where, " AND "))
	var total int
	err := r.db.Get(&total, query, args...)
	return total, err
}

func (r *FormRepo) ListSyncLogsBySource(sourceType string, limit int, offset int, status string, trigger string) ([]models.SyncLogView, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	where := []string{"fr.source_type = $1"}
	args := []interface{}{sourceType}
	argN := 2
	if strings.TrimSpace(status) != "" {
		where = append(where, fmt.Sprintf("l.status = $%d", argN))
		args = append(args, status)
		argN++
	}
	if strings.TrimSpace(trigger) != "" {
		where = append(where, fmt.Sprintf("l.trigger_type = $%d", argN))
		args = append(args, trigger)
		argN++
	}
	args = append(args, limit, offset)
	limitPos := argN
	offsetPos := argN + 1

	var rows []models.SyncLogView
	query := fmt.Sprintf(`
	SELECT l.id, fr.schema_code, fr.display_name, l.trigger_type, l.status, l.started_at, l.finished_at, l.synced_count, l.error_message
	FROM sync_logs l
	JOIN form_registry fr ON fr.id = l.form_id
	WHERE %s
	ORDER BY l.id DESC
	LIMIT $%d OFFSET $%d;
	`, strings.Join(where, " AND "), limitPos, offsetPos)

	err := r.db.Select(&rows, query, args...)
	return rows, err
}

func (r *FormRepo) CountBizRowsSafe(schemaCode string) (int, error) {
	tbl := BizTableName(schemaCode)
	var regClass sql.NullString
	if err := r.db.Get(&regClass, `SELECT to_regclass($1)`, "public."+tbl); err != nil {
		return 0, err
	}
	if !regClass.Valid || regClass.String == "" {
		return 0, nil
	}
	var total int
	if err := r.db.Get(&total, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)); err != nil {
		return 0, err
	}
	return total, nil
}

func (r *FormRepo) ClearBizRows(schemaCode string) error {
	tbl := BizTableName(schemaCode)
	var regClass sql.NullString
	if err := r.db.Get(&regClass, `SELECT to_regclass($1)`, "public."+tbl); err != nil {
		return err
	}
	if !regClass.Valid || regClass.String == "" {
		return nil
	}
	_, err := r.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tbl))
	return err
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

func (r *FormRepo) CountBizRowsForAdmin(schemaCode string, keyword string, field string) (int, error) {
	tbl := BizTableName(schemaCode)
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)
	args := []interface{}{}
	cond, vals := buildAdminFilterCondition(keyword, field)
	if cond != "" {
		q += " WHERE " + cond
		args = append(args, vals...)
	}
	var total int
	if err := r.db.Get(&total, q, args...); err != nil {
		return 0, err
	}
	return total, nil
}

func (r *FormRepo) ListBizRowsForAdmin(schemaCode string, columns []string, keyword string, field string, sortField string, sortOrder string, limit int, offset int) ([]map[string]interface{}, error) {
	tbl := BizTableName(schemaCode)
	selectCols := []string{"object_id", "modified_time"}
	allowedSort := map[string]string{
		"object_id":     quoteIdentifier("object_id"),
		"modified_time": quoteIdentifier("modified_time"),
	}
	for _, c := range columns {
		if isSafeIdentifier(c) {
			selectCols = append(selectCols, quoteIdentifier(c))
			allowedSort[strings.ToLower(c)] = quoteIdentifier(c)
		}
	}

	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectCols, ","), tbl)
	args := []interface{}{}
	cond, vals := buildAdminFilterCondition(keyword, field)
	if cond != "" {
		q += " WHERE " + cond
		args = append(args, vals...)
	}
	sortCol := allowedSort["modified_time"]
	if v, ok := allowedSort[strings.ToLower(strings.TrimSpace(sortField))]; ok {
		sortCol = v
	}
	dir := "DESC"
	if strings.EqualFold(strings.TrimSpace(sortOrder), "ASC") {
		dir = "ASC"
	}
	q += fmt.Sprintf(" ORDER BY %s %s NULLS LAST, %s DESC", sortCol, dir, quoteIdentifier("object_id"))
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

func buildAdminFilterCondition(keyword string, field string) (string, []interface{}) {
	keyword = strings.TrimSpace(keyword)
	field = strings.TrimSpace(field)
	if keyword == "" {
		return "", nil
	}
	if field != "" && field != "__all__" && isSafeIdentifier(field) {
		return fmt.Sprintf("%s ILIKE $1", quoteIdentifier(field)), []interface{}{"%" + keyword + "%"}
	}
	return "raw_json::text ILIKE $1", []interface{}{"%" + keyword + "%"}
}

func (r *FormRepo) GetBySchema(schema string) (models.FormRegistry, error) {
	var row models.FormRegistry
	err := r.db.Get(&row, `
	SELECT id, schema_code, source_type, group_name, display_name, chinese_remark, sync_method, sync_interval_minutes, sync_mode,
	       last_sync_at, last_cursor_modified_time, last_cursor_object_id, is_enabled
	FROM form_registry WHERE schema_code=$1
	`, schema)
	return row, err
}

func (r *FormRepo) UpdateCursor(formID int64, modified *time.Time, objectID *string) error {
	_, err := r.db.Exec(`
	UPDATE form_registry
	SET last_sync_at=now(),
	    last_cursor_modified_time=COALESCE($2, last_cursor_modified_time),
	    last_cursor_object_id=COALESCE($3, last_cursor_object_id),
	    updated_at=now()
	WHERE id=$1
	`, formID, modified, objectID)
	return err
}

func (r *FormRepo) TouchLastSync(formID int64) error {
	_, err := r.db.Exec(`
	UPDATE form_registry
	SET last_sync_at=now(), updated_at=now()
	WHERE id=$1
	`, formID)
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

func (r *FormRepo) MarkTimedOutRunningSyncLogsBySource(sourceType string, before time.Time, reason string) error {
	_, err := r.db.Exec(`
	UPDATE sync_logs l
	SET status='FAILED',
	    error_message=$3,
	    finished_at=now()
	FROM form_registry fr
	WHERE fr.id = l.form_id
	  AND fr.source_type = $1
	  AND l.status = 'RUNNING'
	  AND l.started_at < $2
	`, sourceType, before, reason)
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
		stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s text`, tbl, quoteIdentifier(c))
		if _, err := r.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (r *FormRepo) UpsertBizRow(schemaCode string, objectID string, modifiedTime *time.Time, rawJSON string, fields map[string]string) error {
	tbl := BizTableName(schemaCode)
	cols := []string{
		quoteIdentifier("object_id"),
		quoteIdentifier("modified_time"),
		quoteIdentifier("raw_json"),
		quoteIdentifier("updated_at"),
	}
	vals := []interface{}{objectID, modifiedTime, rawJSON, time.Now().UTC()}
	sets := []string{
		fmt.Sprintf("%s=EXCLUDED.%s", quoteIdentifier("modified_time"), quoteIdentifier("modified_time")),
		fmt.Sprintf("%s=EXCLUDED.%s", quoteIdentifier("raw_json"), quoteIdentifier("raw_json")),
		fmt.Sprintf("%s=EXCLUDED.%s", quoteIdentifier("updated_at"), quoteIdentifier("updated_at")),
	}
	for k, v := range fields {
		if !isSafeIdentifier(k) {
			continue
		}
		qk := quoteIdentifier(k)
		cols = append(cols, qk)
		vals = append(vals, v)
		sets = append(sets, fmt.Sprintf("%s=EXCLUDED.%s", qk, qk))
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

func (r *FormRepo) GetAPIKeyByHash(keyHash string) (models.APIKey, error) {
	var k models.APIKey
	err := r.db.Get(&k, `
	SELECT id, name, remark, key_hash, key_prefix, expires_at, revoked_at, created_at
	FROM api_keys
	WHERE key_hash=$1
	  AND revoked_at IS NULL
	  AND (expires_at IS NULL OR expires_at > now())
	LIMIT 1
	`, keyHash)
	return k, err
}

func (r *FormRepo) ListFormsForAPIKey(keyID int64) ([]models.FormRegistry, error) {
	var forms []models.FormRegistry
	err := r.db.Select(&forms, `
	SELECT fr.id, fr.schema_code, fr.source_type, fr.group_name, fr.display_name, fr.chinese_remark, fr.sync_method, fr.sync_interval_minutes, fr.sync_mode, fr.last_sync_at, fr.last_cursor_modified_time, fr.last_cursor_object_id, fr.is_enabled
	FROM form_registry fr
	JOIN api_key_form_permissions p ON p.form_id=fr.id
	WHERE p.api_key_id=$1
	ORDER BY fr.id ASC
	`, keyID)
	return forms, err
}

func (r *FormRepo) CreateAPIKey(name, remark, keyHash, keyPrefix, keyValue string, expiresAt *time.Time, schemaCodes []string) (int64, error) {
	tx, err := r.db.Beginx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var id int64
	if err := tx.QueryRow(`
	INSERT INTO api_keys(name, remark, key_hash, key_prefix, key_value, expires_at)
	VALUES($1,$2,$3,$4,$5,$6)
	RETURNING id
	`, name, remark, keyHash, keyPrefix, keyValue, expiresAt).Scan(&id); err != nil {
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

func (r *FormRepo) ListAPIKeys() ([]models.APIKeyListView, error) {
	var rows []models.APIKeyListView
	err := r.db.Select(&rows, `
	SELECT ak.id, ak.name, ak.remark, ak.key_prefix, ak.key_value, ak.expires_at, ak.revoked_at, ak.created_at,
	       COALESCE(string_agg(fr.group_name || '/' || fr.display_name, ', ' ORDER BY fr.group_name, fr.display_name), '') AS allowed_forms
	FROM api_keys ak
	LEFT JOIN api_key_form_permissions p ON p.api_key_id = ak.id
	LEFT JOIN form_registry fr ON fr.id = p.form_id
	GROUP BY ak.id
	ORDER BY ak.id DESC;
	`)
	return rows, err
}

func (r *FormRepo) GetAPIKeyPermissions(keyID int64) ([]string, error) {
	var rows []string
	err := r.db.Select(&rows, `
	SELECT fr.schema_code
	FROM api_key_form_permissions p
	JOIN form_registry fr ON fr.id = p.form_id
	WHERE p.api_key_id=$1
	ORDER BY fr.group_name, fr.display_name
	`, keyID)
	return rows, err
}

func (r *FormRepo) UpdateAPIKey(keyID int64, name string, remark string, expiresAt *time.Time, schemaCodes []string) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
	UPDATE api_keys
	SET name=$2, remark=$3, expires_at=$4
	WHERE id=$1
	`, keyID, name, remark, expiresAt); err != nil {
		return err
	}

	if _, err := tx.Exec(`DELETE FROM api_key_form_permissions WHERE api_key_id=$1`, keyID); err != nil {
		return err
	}
	for _, code := range schemaCodes {
		if strings.TrimSpace(code) == "" {
			continue
		}
		if _, err := tx.Exec(`
		INSERT INTO api_key_form_permissions(api_key_id, form_id)
		SELECT $1, id FROM form_registry WHERE schema_code=$2
		`, keyID, code); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *FormRepo) DeleteAPIKey(keyID int64) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM api_key_form_permissions WHERE api_key_id=$1`, keyID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM api_query_logs WHERE api_key_id=$1`, keyID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM api_keys WHERE id=$1`, keyID); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *FormRepo) SetSystemSetting(key string, value string) error {
	_, err := r.db.Exec(`
	INSERT INTO system_settings(key_name, value_text, updated_at)
	VALUES($1,$2,now())
	ON CONFLICT(key_name) DO UPDATE
	SET value_text=EXCLUDED.value_text, updated_at=now()
	`, key, value)
	return err
}

func (r *FormRepo) GetSystemSetting(key string) (string, error) {
	var value string
	err := r.db.Get(&value, `SELECT value_text FROM system_settings WHERE key_name=$1`, key)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func (r *FormRepo) UpsertMSSQLMeta(formID int64, sourceSchema, sourceTable, fullName, incrementalColumn string) error {
	if incrementalColumn == "" {
		incrementalColumn = "id"
	}
	_, err := r.db.Exec(`
	INSERT INTO mssql_form_registry(form_id, source_schema, source_table, source_full_name, incremental_column, last_scanned_at)
	VALUES($1,$2,$3,$4,$5,now())
	ON CONFLICT(form_id) DO UPDATE
	SET source_schema=EXCLUDED.source_schema,
		source_table=EXCLUDED.source_table,
		source_full_name=EXCLUDED.source_full_name,
		incremental_column=EXCLUDED.incremental_column,
		last_scanned_at=now(),
		updated_at=now()
	`, formID, sourceSchema, sourceTable, fullName, incrementalColumn)
	return err
}

func (r *FormRepo) UpdateMSSQLRowMergeConfig(formID int64, enabled bool, uniqueKeyColumn string) error {
	_, err := r.db.Exec(`
	UPDATE mssql_form_registry
	SET stock_update_enabled=$2,
	    unique_key_column=$3,
	    updated_at=now()
	WHERE form_id=$1
	`, formID, enabled, strings.TrimSpace(strings.ToLower(uniqueKeyColumn)))
	return err
}

func (r *FormRepo) TouchMSSQLScannedAt(formID int64) error {
	_, err := r.db.Exec(`UPDATE mssql_form_registry SET last_scanned_at=now(), updated_at=now() WHERE form_id=$1`, formID)
	return err
}

func (r *FormRepo) SetMSSQLLastProcessed(formID int64, filePath string, processedAt *time.Time) error {
	if processedAt == nil {
		now := time.Now().UTC()
		processedAt = &now
	}
	_, err := r.db.Exec(`
	UPDATE mssql_form_registry
	SET last_processed_file=$2, last_processed_at=$3, updated_at=now()
	WHERE form_id=$1
	`, formID, filePath, processedAt)
	return err
}

func (r *FormRepo) InsertMSSQLProcessedFile(formID int64, filePath string, fileName string, fileMode string, fileTime *time.Time, rowCount int, checksum string) error {
	_, err := r.db.Exec(`
	INSERT INTO mssql_processed_files(form_id, file_path, file_name, file_mode, file_time, row_count, checksum)
	VALUES($1,$2,$3,$4,$5,$6,$7)
	ON CONFLICT(form_id, file_path) DO NOTHING
	`, formID, filePath, fileName, fileMode, fileTime, rowCount, checksum)
	return err
}

func (r *FormRepo) IsMSSQLFileProcessed(formID int64, filePath string) (bool, error) {
	var exists int
	err := r.db.Get(&exists, `SELECT 1 FROM mssql_processed_files WHERE form_id=$1 AND file_path=$2`, formID, filePath)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (r *FormRepo) ListMSSQLForms() ([]models.MSSQLFormListView, error) {
	var rows []models.MSSQLFormListView
	err := r.db.Select(&rows, `
	SELECT fr.id AS form_id, fr.schema_code, fr.group_name, fr.display_name, fr.chinese_remark, fr.sync_method, fr.sync_interval_minutes, fr.last_sync_at,
	       mr.source_schema, mr.source_table, mr.source_full_name, mr.incremental_column, mr.stock_update_enabled, mr.unique_key_column, mr.last_processed_file, mr.last_processed_at, mr.last_scanned_at
	FROM form_registry fr
	JOIN mssql_form_registry mr ON mr.form_id = fr.id
	WHERE fr.source_type='MSSQL_BACKUP'
	ORDER BY fr.group_name, fr.display_name, fr.id DESC
	`)
	return rows, err
}

func (r *FormRepo) GetMSSQLMetaBySchema(schemaCode string) (models.MSSQLFormRegistry, error) {
	var row models.MSSQLFormRegistry
	err := r.db.Get(&row, `
	SELECT mr.form_id, mr.source_schema, mr.source_table, mr.source_full_name, mr.incremental_column, mr.stock_update_enabled, mr.unique_key_column,
	       mr.last_processed_file, mr.last_processed_at, mr.last_scanned_at
	FROM mssql_form_registry mr
	JOIN form_registry fr ON fr.id = mr.form_id
	WHERE fr.schema_code=$1
	`, schemaCode)
	return row, err
}

func normalizeMapScan(m map[string]interface{}) map[string]interface{} {
	res := map[string]interface{}{}
	for k, v := range m {
		var strVal string
		switch tv := v.(type) {
		case []byte:
			strVal = string(tv)
		case string:
			strVal = tv
		default:
			res[k] = tv
			continue
		}

		if k == "raw_json" && strVal != "" && (strings.HasPrefix(strVal, "{") || strings.HasPrefix(strVal, "[")) {
			var parsed interface{}
			if err := json.Unmarshal([]byte(strVal), &parsed); err == nil {
				res[k] = parsed
				continue
			}
		}
		res[k] = strVal
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

func quoteIdentifier(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}
