package service

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/HolmesLiu/h3sync/internal/repository"
	"go.uber.org/zap"
)

const settingMSSQLBackupRootPath = "mssql_backup_root_path"

var sqlFileNamePattern = regexp.MustCompile(`^(\d{6})_(FULL|INCR)_([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.sql$`)
var valuesMarkerPattern = regexp.MustCompile(`(?i)\)\s*VALUES\s*\(`)

type mssqlSQLFile struct {
	Path       string
	Name       string
	Mode       string
	SchemaName string
	TableName  string
	FullName   string
	FileTime   time.Time
}

type MSSQLBackupService struct {
	repo   *repository.FormRepo
	logger *zap.Logger
}

func NewMSSQLBackupService(repo *repository.FormRepo, logger *zap.Logger) *MSSQLBackupService {
	return &MSSQLBackupService{repo: repo, logger: logger}
}

func (s *MSSQLBackupService) GetBackupRootPath() (string, error) {
	return s.repo.GetSystemSetting(settingMSSQLBackupRootPath)
}

func (s *MSSQLBackupService) SetBackupRootPath(path string) error {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return fmt.Errorf("备份路径不能为空")
	}
	st, err := os.Stat(clean)
	if err != nil {
		return fmt.Errorf("备份路径不可用: %w", err)
	}
	if !st.IsDir() {
		return fmt.Errorf("备份路径必须是目录")
	}
	return s.repo.SetSystemSetting(settingMSSQLBackupRootPath, clean)
}

func (s *MSSQLBackupService) DiscoverForms(ctx context.Context) (int, int, error) {
	_ = ctx
	root, err := s.GetBackupRootPath()
	if err != nil {
		return 0, 0, err
	}
	if root == "" {
		return 0, 0, fmt.Errorf("请先配置表单库路径")
	}

	files, err := discoverSQLFiles(root)
	if err != nil {
		return 0, 0, err
	}
	if len(files) == 0 {
		return 0, 0, nil
	}

	byTable := map[string]mssqlSQLFile{}
	for _, f := range files {
		exist, ok := byTable[f.FullName]
		if !ok || f.FileTime.After(exist.FileTime) {
			byTable[f.FullName] = f
		}
	}

	created := 0
	updated := 0
	for _, sample := range byTable {
		schemaCode := mssqlSchemaCode(sample.SchemaName, sample.TableName)
		form, err := s.repo.GetBySchema(schemaCode)
		if err != nil {
			form = models.FormRegistry{
				SchemaCode:          schemaCode,
				SourceType:          "MSSQL_BACKUP",
				GroupName:           "MSSQL默认分组",
				DisplayName:         sample.TableName,
				ChineseRemark:       sample.FullName,
				SyncMethod:          "MANUAL",
				SyncIntervalMinutes: 60,
				SyncMode:            "INCREMENTAL",
				IsEnabled:           true,
			}
			if err := s.repo.Upsert(form); err != nil {
				return created, updated, err
			}
			created++
			form, err = s.repo.GetBySchema(schemaCode)
			if err != nil {
				return created, updated, err
			}
		} else {
			updated++
		}

		if err := s.repo.UpsertMSSQLMeta(form.ID, sample.SchemaName, sample.TableName, sample.FullName, "id"); err != nil {
			return created, updated, err
		}
	}

	return created, updated, nil
}

func (s *MSSQLBackupService) SyncForm(ctx context.Context, form models.FormRegistry) (int, error) {
	_ = ctx
	meta, err := s.repo.GetMSSQLMetaBySchema(form.SchemaCode)
	if err != nil {
		return 0, err
	}
	root, err := s.GetBackupRootPath()
	if err != nil {
		return 0, err
	}
	if root == "" {
		return 0, fmt.Errorf("请先配置表单库路径")
	}

	files, err := discoverSQLFiles(root)
	if err != nil {
		return 0, err
	}

	matched := make([]mssqlSQLFile, 0)
	for _, f := range files {
		if strings.EqualFold(f.FullName, meta.SourceFullName) {
			matched = append(matched, f)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].FileTime.Equal(matched[j].FileTime) {
			return matched[i].Path < matched[j].Path
		}
		return matched[i].FileTime.Before(matched[j].FileTime)
	})

	totalRows := 0
	for _, f := range matched {
		processed, err := s.repo.IsMSSQLFileProcessed(form.ID, f.Path)
		if err != nil {
			return totalRows, err
		}
		if processed {
			continue
		}

		rowCount, checksum, err := s.importSQLFile(form, meta, f)
		if err != nil {
			return totalRows, fmt.Errorf("导入文件失败 %s: %w", f.Name, err)
		}
		totalRows += rowCount

		if err := s.repo.InsertMSSQLProcessedFile(form.ID, f.Path, f.Name, f.Mode, &f.FileTime, rowCount, checksum); err != nil {
			return totalRows, err
		}
		if err := s.repo.SetMSSQLLastProcessed(form.ID, f.Path, &f.FileTime); err != nil {
			return totalRows, err
		}
	}

	if err := s.repo.TouchMSSQLScannedAt(form.ID); err != nil {
		return totalRows, err
	}
	if err := s.repo.TouchLastSync(form.ID); err != nil {
		return totalRows, err
	}
	return totalRows, nil
}

func (s *MSSQLBackupService) importSQLFile(form models.FormRegistry, meta models.MSSQLFormRegistry, file mssqlSQLFile) (int, string, error) {
	b, err := os.ReadFile(file.Path)
	if err != nil {
		return 0, "", err
	}

	sum := sha1.Sum(b)
	checksum := hex.EncodeToString(sum[:])

	lines := strings.Split(string(b), "\n")
	rowCount := 0
	parseErrCount := 0
	parseErrSamples := make([]string, 0, 5)
	columnSet := map[string]struct{}{}
	parsedRows := make([]map[string]string, 0, len(lines)/2)

	pending := ""
	pendingLine := 0
	processStatement := func(stmt string, lineNo int) {
		row, err := parseInsertLine(stmt)
		if err != nil {
			parseErrCount++
			if len(parseErrSamples) < 5 {
				parseErrSamples = append(parseErrSamples, fmt.Sprintf("line=%d err=%v", lineNo, err))
			}
			return
		}
		for k := range row {
			columnSet[k] = struct{}{}
		}
		parsedRows = append(parsedRows, row)
	}

	for i, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		u := strings.ToUpper(line)
		if pending == "" {
			if strings.HasPrefix(u, "INSERT INTO") {
				pending = line
				pendingLine = i + 1
				if strings.HasSuffix(pending, ");") {
					processStatement(pending, pendingLine)
					pending = ""
					pendingLine = 0
				}
			}
			continue
		}

		pending += " " + line
		if strings.HasSuffix(strings.TrimSpace(pending), ");") {
			processStatement(pending, pendingLine)
			pending = ""
			pendingLine = 0
		}
	}
	if pending != "" {
		processStatement(pending, pendingLine)
	}
	if parseErrCount > 0 {
		s.logger.Warn("mssql sql parse has bad rows; skipped",
			zap.String("schema", form.SchemaCode),
			zap.String("file", file.Path),
			zap.Int("bad_rows", parseErrCount),
			zap.Strings("samples", parseErrSamples))
	}
	if len(parsedRows) == 0 {
		if parseErrCount > 0 {
			return 0, checksum, fmt.Errorf("文件无法解析，坏行数=%d", parseErrCount)
		}
		return 0, checksum, nil
	}

	columns := make([]string, 0, len(columnSet))
	for k := range columnSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)
	if err := s.repo.EnsureBizTable(form.SchemaCode, columns); err != nil {
		return rowCount, checksum, err
	}

	incCol := strings.ToLower(strings.TrimSpace(meta.IncrementalColumn))
	if incCol == "" {
		incCol = "id"
	}

	for i, row := range parsedRows {
		objID := strings.TrimSpace(row[incCol])
		if objID == "" {
			objID = strings.TrimSpace(row["id"])
		}
		if objID == "" {
			objID = fmt.Sprintf("%s#%d", file.Name, i+1)
		}

		modified := inferModifiedTime(row)
		rawJSON, _ := json.Marshal(row)
		if err := s.repo.UpsertBizRow(form.SchemaCode, objID, modified, string(rawJSON), row); err != nil {
			return rowCount, checksum, err
		}
		rowCount++
	}

	return rowCount, checksum, nil
}

func discoverSQLFiles(root string) ([]mssqlSQLFile, error) {
	files := make([]mssqlSQLFile, 0, 128)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(d.Name()), ".sql") {
			return nil
		}
		meta, ok := parseSQLFileName(path)
		if ok {
			files = append(files, meta)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func parseSQLFileName(path string) (mssqlSQLFile, bool) {
	base := filepath.Base(path)
	m := sqlFileNamePattern.FindStringSubmatch(base)
	if len(m) != 5 {
		return mssqlSQLFile{}, false
	}

	dirName := filepath.Base(filepath.Dir(path))
	d, err := time.Parse("2006-01-02", dirName)
	if err != nil {
		d = time.Now()
	}
	hhmmss := m[1]
	hour := toInt(hhmmss[0:2])
	minute := toInt(hhmmss[2:4])
	second := toInt(hhmmss[4:6])
	fileTime := time.Date(d.Year(), d.Month(), d.Day(), hour, minute, second, 0, time.Local)

	schemaName := m[3]
	tableName := m[4]
	return mssqlSQLFile{
		Path:       path,
		Name:       base,
		Mode:       strings.ToUpper(m[2]),
		SchemaName: schemaName,
		TableName:  tableName,
		FullName:   strings.ToLower(schemaName + "." + tableName),
		FileTime:   fileTime,
	}, true
}

func parseInsertLine(line string) (map[string]string, error) {
	loc := valuesMarkerPattern.FindStringIndex(line)
	if len(loc) != 2 {
		return nil, fmt.Errorf("invalid insert statement")
	}
	idxValues := loc[0]
	valuesStart := loc[1]
	idxColsStart := strings.Index(line, "(")
	if idxColsStart < 0 || idxColsStart >= idxValues {
		return nil, fmt.Errorf("invalid column segment")
	}
	colsPart := line[idxColsStart+1 : idxValues]

	valuesPart := line[valuesStart:]
	valuesPart = strings.TrimSuffix(valuesPart, ");")
	valuesPart = strings.TrimSuffix(valuesPart, ")")

	cols := splitByCommaSafe(colsPart)
	vals := splitByCommaSafe(valuesPart)
	if len(cols) != len(vals) {
		v2, ok := reconcileValueParts(vals, len(cols))
		if !ok {
			return nil, fmt.Errorf("column/value length mismatch")
		}
		vals = v2
	}

	row := make(map[string]string, len(cols))
	for i := range cols {
		c := normalizeSQLIdentifier(cols[i])
		if c == "" {
			continue
		}
		row[strings.ToLower(c)] = normalizeSQLValue(vals[i])
	}
	return row, nil
}

func reconcileValueParts(vals []string, target int) ([]string, bool) {
	if target <= 0 {
		return nil, false
	}
	if len(vals) == target {
		return vals, true
	}
	if len(vals) > target {
		merged := make([]string, 0, target)
		merged = append(merged, vals[:target-1]...)
		merged = append(merged, strings.Join(vals[target-1:], ","))
		return merged, true
	}
	padded := make([]string, target)
	copy(padded, vals)
	for i := len(vals); i < target; i++ {
		padded[i] = "NULL"
	}
	return padded, true
}

func splitByCommaSafe(input string) []string {
	parts := make([]string, 0, 16)
	var b strings.Builder
	inQuote := false

	for i := 0; i < len(input); i++ {
		ch := input[i]
		if ch == '\'' {
			if inQuote && i+1 < len(input) && input[i+1] == '\'' {
				b.WriteByte(ch)
				b.WriteByte(input[i+1])
				i++
				continue
			}
			inQuote = !inQuote
			b.WriteByte(ch)
			continue
		}
		if ch == ',' && !inQuote {
			parts = append(parts, strings.TrimSpace(b.String()))
			b.Reset()
			continue
		}
		b.WriteByte(ch)
	}
	parts = append(parts, strings.TrimSpace(b.String()))
	return parts
}

func normalizeSQLIdentifier(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, "[]")
	v = strings.Trim(v, "`")
	v = strings.Trim(v, `"`)
	return v
}

func normalizeSQLValue(v string) string {
	v = strings.TrimSpace(v)
	if strings.EqualFold(v, "NULL") {
		return ""
	}
	if strings.HasPrefix(v, "N'") && strings.HasSuffix(v, "'") {
		v = v[2 : len(v)-1]
		return strings.ReplaceAll(v, "''", "'")
	}
	if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
		v = v[1 : len(v)-1]
		return strings.ReplaceAll(v, "''", "'")
	}
	return v
}

func inferModifiedTime(row map[string]string) *time.Time {
	candidates := []string{"modified_time", "updated_at", "update_time", "last_update_time", "send_date", "created_at"}
	for _, key := range candidates {
		if raw := strings.TrimSpace(row[key]); raw != "" {
			t := parseDateTime(raw)
			if t != nil {
				return t
			}
		}
	}
	return nil
}

func parseDateTime(raw string) *time.Time {
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, raw, time.Local); err == nil {
			u := t.UTC()
			return &u
		}
	}
	return nil
}

func mssqlSchemaCode(schemaName, tableName string) string {
	return "mssql_" + sanitizeToken(schemaName) + "_" + sanitizeToken(tableName)
}

func sanitizeToken(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	var b strings.Builder
	for _, ch := range v {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			b.WriteRune(ch)
			continue
		}
		b.WriteByte('_')
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "x"
	}
	return out
}

func toInt(v string) int {
	n := 0
	for i := 0; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return 0
		}
		n = n*10 + int(v[i]-'0')
	}
	return n
}
