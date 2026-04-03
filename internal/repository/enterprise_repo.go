package repository

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/HolmesLiu/h3sync/internal/models"
)

func (r *FormRepo) CreateEnterpriseLibraryMeta(meta models.EnterpriseLibraryForm) error {
	_, err := r.db.Exec(`
	INSERT INTO enterprise_library_forms(form_id, source_filename, stored_filename, sheet_name, file_ext, mime_type, file_size, source_checksum, file_checksum, file_path, parse_status, parse_message, row_count, parsed_at)
	VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`, meta.FormID, meta.SourceFilename, meta.StoredFilename, meta.SheetName, meta.FileExt, meta.MimeType, meta.FileSize, meta.SourceChecksum, meta.FileChecksum, meta.FilePath, meta.ParseStatus, meta.ParseMessage, meta.RowCount, meta.ParsedAt)
	return err
}

func (r *FormRepo) UpdateEnterpriseLibraryMeta(formID int64, parseStatus string, parseMessage string, rowCount int, parsedAt *time.Time) error {
	_, err := r.db.Exec(`
	UPDATE enterprise_library_forms
	SET parse_status=$2,
	    parse_message=$3,
	    row_count=$4,
	    parsed_at=$5,
	    updated_at=now()
	WHERE form_id=$1
	`, formID, parseStatus, parseMessage, rowCount, parsedAt)
	return err
}

func (r *FormRepo) GetEnterpriseLibraryMetaBySchema(schemaCode string) (models.EnterpriseLibraryForm, error) {
	var row models.EnterpriseLibraryForm
	err := r.db.Get(&row, `
	SELECT elf.form_id, elf.source_filename, elf.stored_filename, elf.sheet_name, elf.file_ext, elf.mime_type, elf.file_size, elf.source_checksum, elf.file_checksum, elf.file_path,
	       elf.parse_status, elf.parse_message, elf.row_count, elf.parsed_at
	FROM enterprise_library_forms elf
	JOIN form_registry fr ON fr.id = elf.form_id
	WHERE fr.schema_code=$1
	`, schemaCode)
	return row, err
}

func (r *FormRepo) ListEnterpriseLibraryForms() ([]models.EnterpriseLibraryListView, error) {
	var rows []models.EnterpriseLibraryListView
	err := r.db.Select(&rows, `
	SELECT fr.id AS form_id, fr.schema_code, fr.group_name, fr.display_name, fr.chinese_remark, fr.last_sync_at,
	       elf.source_filename, elf.sheet_name, elf.file_ext, elf.mime_type, elf.file_size, elf.source_checksum, elf.file_checksum, elf.parse_status, elf.parse_message, elf.row_count, elf.parsed_at
	FROM form_registry fr
	JOIN enterprise_library_forms elf ON elf.form_id = fr.id
	WHERE fr.source_type='ENTERPRISE_LIBRARY'
	ORDER BY fr.group_name, fr.display_name, fr.id DESC
	`)
	return rows, err
}

func (r *FormRepo) EnterpriseLibrarySourceChecksumExists(checksum string) (bool, error) {
	var exists int
	err := r.db.Get(&exists, `SELECT 1 FROM enterprise_library_forms WHERE source_checksum=$1 LIMIT 1`, checksum)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (r *FormRepo) EnterpriseLibraryFileReferenceCount(filePath string) (int, error) {
	var total int
	err := r.db.Get(&total, `SELECT COUNT(*) FROM enterprise_library_forms WHERE file_path=$1`, filePath)
	return total, err
}

func (r *FormRepo) DeleteEnterpriseLibraryByChecksum(checksum string) error {
	type existing struct {
		SchemaCode string `db:"schema_code"`
		FilePath   string `db:"file_path"`
	}
	var row existing
	err := r.db.Get(&row, `
	SELECT fr.schema_code, elf.file_path
	FROM enterprise_library_forms elf
	JOIN form_registry fr ON fr.id = elf.form_id
	WHERE elf.file_checksum=$1
	LIMIT 1
	`, checksum)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if err := r.DeleteBySchema(row.SchemaCode); err != nil {
		return err
	}
	if strings.TrimSpace(row.FilePath) != "" {
		_ = os.Remove(row.FilePath)
	}
	return nil
}

func (r *FormRepo) HasEnterpriseLibraryProcessing() (bool, error) {
	var exists int
	err := r.db.Get(&exists, `SELECT 1 FROM enterprise_library_forms WHERE parse_status='PROCESSING' LIMIT 1`)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (r *FormRepo) DeleteBizRow(schemaCode string, objectID string) error {
	tbl := BizTableName(schemaCode)
	_, err := r.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE object_id=$1`, tbl), objectID)
	return err
}

func (r *FormRepo) GetBizRowByObjectID(schemaCode string, columns []string, objectID string) (map[string]interface{}, error) {
	tbl := BizTableName(schemaCode)
	selectCols := []string{"object_id", "modified_time"}
	for _, c := range columns {
		if isSafeIdentifier(c) {
			selectCols = append(selectCols, quoteIdentifier(c))
		}
	}
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE object_id=$1 LIMIT 1`, strings.Join(selectCols, ","), tbl)
	row := map[string]interface{}{}
	rows, err := r.db.Queryx(query, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	if err := rows.MapScan(row); err != nil {
		return nil, err
	}
	return normalizeMapScan(row), nil
}
