package service

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/HolmesLiu/h3sync/internal/repository"
	"github.com/ledongthuc/pdf"
	"github.com/nguyenthenguyen/docx"
	"github.com/xuri/excelize/v2"
	"go.uber.org/zap"
)

const enterpriseUploadDir = "uploads/enterprise_library"
const enterpriseObjectIDKey = "__object_id"

type EnterpriseLibraryService struct {
	repo *repository.FormRepo
	log  *zap.Logger
	mu   sync.Mutex
}

type EnterpriseUploadInput struct {
	DisplayName   string
	ChineseRemark string
	GroupName     string
	FileHeader    *multipart.FileHeader
}

type EnterpriseUploadResult struct {
	CreatedSchemas []string
}

type EnterpriseImportInput struct {
	SchemaCode string
	FileHeader *multipart.FileHeader
}

type enterpriseColumn struct {
	Code  string
	Label string
}

type enterpriseDataset struct {
	SheetName string
	Columns   []enterpriseColumn
	Rows      []map[string]string
}

func NewEnterpriseLibraryService(repo *repository.FormRepo, log *zap.Logger) *EnterpriseLibraryService {
	return &EnterpriseLibraryService{repo: repo, log: log}
}

func (s *EnterpriseLibraryService) UploadAndImport(input EnterpriseUploadInput) (EnterpriseUploadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fileBytes, ext, checksum, err := s.readUploadInput(input.FileHeader)
	if err != nil {
		return EnterpriseUploadResult{}, err
	}
	if strings.TrimSpace(input.DisplayName) == "" {
		return EnterpriseUploadResult{}, fmt.Errorf("表单名称不能为空")
	}
	if err := s.ensureNoProcessing(); err != nil {
		return EnterpriseUploadResult{}, err
	}
	exists, err := s.repo.EnterpriseLibrarySourceChecksumExists(checksum)
	if err != nil {
		return EnterpriseUploadResult{}, err
	}
	if exists {
		return EnterpriseUploadResult{}, fmt.Errorf("该文件已经上传过，不能重复导入；如需重传，请先删除原资料表")
	}

	storedFilename, storedPath, err := writeEnterpriseUploadFile(input.FileHeader.Filename, ext, fileBytes)
	if err != nil {
		return EnterpriseUploadResult{}, err
	}

	datasets, err := parseEnterpriseDatasetsFromFile(storedPath, ext)
	if err != nil {
		_ = os.Remove(storedPath)
		return EnterpriseUploadResult{}, err
	}

	groupName := strings.TrimSpace(input.GroupName)
	if groupName == "" {
		groupName = "企业资料库"
	}

	createdSchemas := make([]string, 0, len(datasets))
	cleanupSchemas := make([]string, 0, len(datasets))
	for idx, dataset := range datasets {
		displayName := buildEnterpriseDisplayName(strings.TrimSpace(input.DisplayName), idx, len(datasets))
		schemaCode := uniqueEnterpriseSchemaCode(displayName)
		form := models.FormRegistry{
			SchemaCode:          schemaCode,
			SourceType:          "ENTERPRISE_LIBRARY",
			GroupName:           groupName,
			DisplayName:         displayName,
			ChineseRemark:       strings.TrimSpace(input.ChineseRemark),
			SyncMethod:          "MANUAL",
			SyncIntervalMinutes: 0,
			SyncMode:            "FULL",
			IsEnabled:           true,
		}
		if err := s.repo.Upsert(form); err != nil {
			s.cleanupEnterpriseUpload(cleanupSchemas, storedPath)
			return EnterpriseUploadResult{}, err
		}
		form, err = s.repo.GetBySchema(schemaCode)
		if err != nil {
			s.cleanupEnterpriseUpload(cleanupSchemas, storedPath)
			return EnterpriseUploadResult{}, err
		}
		cleanupSchemas = append(cleanupSchemas, schemaCode)

		meta := models.EnterpriseLibraryForm{
			FormID:         form.ID,
			SourceFilename: input.FileHeader.Filename,
			StoredFilename: storedFilename,
			SheetName:      dataset.SheetName,
			FileExt:        ext,
			MimeType:       input.FileHeader.Header.Get("Content-Type"),
			FileSize:       int64(len(fileBytes)),
			SourceChecksum: checksum,
			FileChecksum:   buildEnterpriseFormChecksum(checksum, dataset.SheetName, idx),
			FilePath:       storedPath,
			ParseStatus:    "PROCESSING",
			ParseMessage:   "文件解析中",
		}
		if err := s.repo.CreateEnterpriseLibraryMeta(meta); err != nil {
			s.cleanupEnterpriseUpload(cleanupSchemas, storedPath)
			return EnterpriseUploadResult{}, err
		}

		if err := s.writeParsedForm(form, dataset); err != nil {
			_ = s.repo.UpdateEnterpriseLibraryMeta(form.ID, "FAILED", err.Error(), 0, nil)
			s.cleanupEnterpriseUpload(cleanupSchemas, storedPath)
			return EnterpriseUploadResult{}, err
		}

		now := time.Now().UTC()
		if err := s.repo.UpdateEnterpriseLibraryMeta(form.ID, "READY", "解析完成", len(dataset.Rows), &now); err != nil {
			s.cleanupEnterpriseUpload(cleanupSchemas, storedPath)
			return EnterpriseUploadResult{}, err
		}
		if err := s.repo.TouchLastSync(form.ID); err != nil {
			s.cleanupEnterpriseUpload(cleanupSchemas, storedPath)
			return EnterpriseUploadResult{}, err
		}
		createdSchemas = append(createdSchemas, schemaCode)
	}

	return EnterpriseUploadResult{CreatedSchemas: createdSchemas}, nil
}

func (s *EnterpriseLibraryService) ImportIntoForm(input EnterpriseImportInput) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.TrimSpace(input.SchemaCode) == "" {
		return fmt.Errorf("资料表不存在")
	}
	if err := s.ensureNoProcessing(); err != nil {
		return err
	}

	form, err := s.repo.GetBySchema(input.SchemaCode)
	if err != nil || !strings.EqualFold(form.SourceType, "ENTERPRISE_LIBRARY") {
		return fmt.Errorf("资料表不存在")
	}

	fileBytes, ext, _, err := s.readUploadInput(input.FileHeader)
	if err != nil {
		return err
	}

	_, storedPath, err := writeEnterpriseUploadFile(input.FileHeader.Filename, ext, fileBytes)
	if err != nil {
		return err
	}
	defer os.Remove(storedPath)

	datasets, err := parseEnterpriseDatasetsFromFile(storedPath, ext)
	if err != nil {
		return err
	}
	if len(datasets) != 1 {
		return fmt.Errorf("追加导入仅支持单表文件；Excel 只能包含一个有效 Sheet")
	}

	columns, err := s.repo.ListBizColumns(form.SchemaCode)
	if err != nil {
		return err
	}
	fieldRemarks, err := s.repo.ListFieldRemarks(form.ID)
	if err != nil {
		return err
	}
	if err := validateEnterpriseImportDataset(datasets[0], columns, fieldRemarks); err != nil {
		return err
	}
	return s.writeImportedRows(form.SchemaCode, datasets[0], columns, fieldRemarks)
}

func (s *EnterpriseLibraryService) ensureNoProcessing() error {
	processing, err := s.repo.HasEnterpriseLibraryProcessing()
	if err != nil {
		return err
	}
	if processing {
		return fmt.Errorf("当前仍有文件正在解析，请等待完成后再操作")
	}
	return nil
}

func (s *EnterpriseLibraryService) readUploadInput(fileHeader *multipart.FileHeader) ([]byte, string, string, error) {
	if fileHeader == nil {
		return nil, "", "", fmt.Errorf("请选择上传文件")
	}
	src, err := fileHeader.Open()
	if err != nil {
		return nil, "", "", err
	}
	defer src.Close()

	fileBytes, err := io.ReadAll(src)
	if err != nil {
		return nil, "", "", err
	}
	if len(fileBytes) == 0 {
		return nil, "", "", fmt.Errorf("上传文件不能为空")
	}
	ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
	if !isSupportedEnterpriseExt(ext) {
		return nil, "", "", fmt.Errorf("不支持的文件格式: %s", ext)
	}
	if ext == ".doc" {
		return nil, "", "", fmt.Errorf("暂不支持旧版 .doc，请转换为 .docx 后上传")
	}
	return fileBytes, ext, checksumBytes(fileBytes), nil
}

func (s *EnterpriseLibraryService) cleanupEnterpriseUpload(schemas []string, storedPath string) {
	for _, schema := range schemas {
		_ = s.repo.DeleteBySchema(schema)
	}
	if strings.TrimSpace(storedPath) != "" {
		_ = os.Remove(storedPath)
	}
}

func (s *EnterpriseLibraryService) writeParsedForm(form models.FormRegistry, dataset enterpriseDataset) error {
	if len(dataset.Columns) == 0 {
		return fmt.Errorf("未解析出可用字段")
	}
	columnCodes := datasetColumnCodes(dataset.Columns)
	if err := s.repo.EnsureBizTable(form.SchemaCode, columnCodes); err != nil {
		return err
	}
	for _, col := range dataset.Columns {
		name := strings.TrimSpace(col.Label)
		if name == "" {
			name = col.Code
		}
		_ = s.repo.UpsertFieldRemark(form.ID, models.FormFieldRegistry{
			FormID:        form.ID,
			FieldCode:     col.Code,
			FieldName:     name,
			ChineseRemark: name,
			ShowInAdmin:   true,
			OriginalType:  "text",
			StorageType:   "text",
		})
	}
	return s.writeRows(form.SchemaCode, dataset.Rows)
}

func (s *EnterpriseLibraryService) writeImportedRows(schemaCode string, dataset enterpriseDataset, expectedColumns []string, remarks []models.FormFieldRegistry) error {
	mappedRows, err := remapImportRows(dataset, expectedColumns, remarks)
	if err != nil {
		return err
	}
	return s.writeRows(schemaCode, mappedRows)
}

func (s *EnterpriseLibraryService) writeRows(schemaCode string, rows []map[string]string) error {
	for idx, row := range rows {
		objectID := strings.TrimSpace(row[enterpriseObjectIDKey])
		if objectID == "" {
			objectID = fmt.Sprintf("row_%06d", idx+1)
		}
		cleanRow := map[string]string{}
		for key, value := range row {
			if key == enterpriseObjectIDKey {
				continue
			}
			cleanRow[key] = value
		}
		raw, _ := json.Marshal(cleanRow)
		now := time.Now().UTC()
		if err := s.repo.UpsertBizRow(schemaCode, objectID, &now, string(raw), cleanRow); err != nil {
			return err
		}
	}
	return nil
}

func parseEnterpriseDatasetsFromFile(path string, ext string) ([]enterpriseDataset, error) {
	switch ext {
	case ".csv":
		ds, err := parseCSVFile(path)
		if err != nil {
			return nil, err
		}
		return []enterpriseDataset{ds}, nil
	case ".txt":
		ds, err := parseTextFile(path)
		if err != nil {
			return nil, err
		}
		return []enterpriseDataset{ds}, nil
	case ".xlsx", ".xlsm", ".xltx":
		return parseExcelFile(path)
	case ".docx":
		ds, err := parseDocxFile(path)
		if err != nil {
			return nil, err
		}
		return []enterpriseDataset{ds}, nil
	case ".pdf":
		ds, err := parsePDFFile(path)
		if err != nil {
			return nil, err
		}
		return []enterpriseDataset{ds}, nil
	default:
		return nil, fmt.Errorf("不支持的文件格式: %s", ext)
	}
}

func parseCSVFile(path string) (enterpriseDataset, error) {
	f, err := os.Open(path)
	if err != nil {
		return enterpriseDataset{}, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return enterpriseDataset{}, err
	}
	return parseTabularRecords(records)
}

func parseExcelFile(path string) ([]enterpriseDataset, error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("Excel 文件没有工作表")
	}

	out := make([]enterpriseDataset, 0, len(sheets))
	for _, sheet := range sheets {
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		if countNonEmptyRows(rows) == 0 {
			continue
		}
		parsed, err := parseExcelSheetRecords(rows)
		if err != nil {
			continue
		}
		if len(parsed.Rows) == 0 {
			continue
		}
		parsed.SheetName = sheet
		out = append(out, parsed)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("Excel 文件未解析出可用数据")
	}
	return out, nil
}

func parseTextFile(path string) (enterpriseDataset, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return enterpriseDataset{}, err
	}
	return parseTextContent(string(b))
}

func parseDocxFile(path string) (enterpriseDataset, error) {
	r, err := docx.ReadDocxFile(path)
	if err != nil {
		return enterpriseDataset{}, err
	}
	defer r.Close()

	content := extractDocxText(r.Editable().GetContent())
	return parseTextContent(content)
}

func parsePDFFile(path string) (enterpriseDataset, error) {
	f, r, err := pdf.Open(path)
	if err != nil {
		return enterpriseDataset{}, err
	}
	defer f.Close()

	textReader, err := r.GetPlainText()
	if err != nil {
		return enterpriseDataset{}, err
	}
	b, err := io.ReadAll(textReader)
	if err != nil {
		return enterpriseDataset{}, err
	}
	return parseTextContent(string(b))
}

func parseTabularRecords(records [][]string) (enterpriseDataset, error) {
	if len(records) == 0 {
		return enterpriseDataset{}, fmt.Errorf("文件内容为空")
	}
	headerIndex := firstNonEmptyRowIndex(records)
	if headerIndex < 0 {
		return enterpriseDataset{}, fmt.Errorf("文件内容为空")
	}
	maxColumns := maxRecordWidth(records[headerIndex:])
	columns := buildColumns(records[headerIndex], maxColumns)
	rows := buildRowsFromRecords(records[headerIndex+1:], columns)
	if len(rows) == 0 {
		return enterpriseDataset{}, fmt.Errorf("未解析出有效数据行")
	}
	return enterpriseDataset{Columns: columns, Rows: rows}, nil
}

func parseExcelSheetRecords(records [][]string) (enterpriseDataset, error) {
	headerIndex := bestExcelHeaderRowIndex(records)
	if headerIndex < 0 {
		return enterpriseDataset{}, fmt.Errorf("sheet is empty")
	}
	maxColumns := maxRecordWidth(records[headerIndex:])
	columns := buildColumns(records[headerIndex], maxColumns)
	rows := buildRowsFromRecords(records[headerIndex+1:], columns)
	if len(rows) == 0 {
		return enterpriseDataset{}, fmt.Errorf("sheet has no data rows")
	}
	return enterpriseDataset{Columns: columns, Rows: rows}, nil
}

func buildColumns(raw []string, width int) []enterpriseColumn {
	if width < len(raw) {
		width = len(raw)
	}
	seen := map[string]int{}
	columns := make([]enterpriseColumn, 0, width)
	for idx := 0; idx < width; idx++ {
		label := ""
		if idx < len(raw) {
			label = strings.TrimSpace(raw[idx])
		}
		code := sanitizeColumnName(label)
		if code == "" {
			code = fmt.Sprintf("column_%d", idx+1)
		}
		if count := seen[code]; count > 0 {
			code = fmt.Sprintf("%s_%d", code, count+1)
		}
		seen[code]++
		if label == "" {
			label = fmt.Sprintf("字段%d", idx+1)
		}
		columns = append(columns, enterpriseColumn{Code: code, Label: label})
	}
	return columns
}

func buildRowsFromRecords(records [][]string, columns []enterpriseColumn) []map[string]string {
	rows := make([]map[string]string, 0)
	for idx, record := range records {
		row := map[string]string{}
		empty := true
		for colIdx, col := range columns {
			value := ""
			if colIdx < len(record) {
				value = strings.TrimSpace(record[colIdx])
			}
			if value != "" {
				empty = false
			}
			row[col.Code] = value
		}
		if empty {
			continue
		}
		row[enterpriseObjectIDKey] = fmt.Sprintf("row_%06d", idx+1)
		rows = append(rows, row)
	}
	return rows
}

func parseTextContent(content string) (enterpriseDataset, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return enterpriseDataset{}, fmt.Errorf("文件内容为空")
	}

	parts := splitTextChunks(content)
	rows := make([]map[string]string, 0, len(parts))
	for idx, part := range parts {
		rows = append(rows, map[string]string{
			enterpriseObjectIDKey: fmt.Sprintf("chunk_%06d", idx+1),
			"title":               fmt.Sprintf("片段 %d", idx+1),
			"content":             part,
		})
	}
	return enterpriseDataset{
		Columns: []enterpriseColumn{
			{Code: "title", Label: "标题"},
			{Code: "content", Label: "内容"},
		},
		Rows: rows,
	}, nil
}

func validateEnterpriseImportDataset(dataset enterpriseDataset, expectedColumns []string, remarks []models.FormFieldRegistry) error {
	if len(dataset.Columns) != len(expectedColumns) {
		return fmt.Errorf("导入文件字段数量与当前表单不一致")
	}
	resolved, err := resolveDatasetColumns(dataset.Columns, expectedColumns, remarks)
	if err != nil {
		return err
	}
	for idx, expected := range expectedColumns {
		if resolved[idx] != expected {
			return fmt.Errorf("导入文件字段顺序或名称不匹配，请按当前表单字段格式导入")
		}
	}
	return nil
}

func remapImportRows(dataset enterpriseDataset, expectedColumns []string, remarks []models.FormFieldRegistry) ([]map[string]string, error) {
	resolved, err := resolveDatasetColumns(dataset.Columns, expectedColumns, remarks)
	if err != nil {
		return nil, err
	}
	codeMap := map[string]string{}
	for idx, col := range dataset.Columns {
		codeMap[col.Code] = resolved[idx]
	}

	rows := make([]map[string]string, 0, len(dataset.Rows))
	for idx, row := range dataset.Rows {
		mapped := map[string]string{
			enterpriseObjectIDKey: fmt.Sprintf("import_%d_%d", time.Now().UnixNano(), idx+1),
		}
		for sourceCode, targetCode := range codeMap {
			mapped[targetCode] = row[sourceCode]
		}
		rows = append(rows, mapped)
	}
	return rows, nil
}

func resolveDatasetColumns(columns []enterpriseColumn, expectedColumns []string, remarks []models.FormFieldRegistry) ([]string, error) {
	aliasToCode := map[string]string{}
	for _, code := range expectedColumns {
		aliasToCode[normalizeHeaderAlias(code)] = code
	}
	for _, remark := range remarks {
		if strings.TrimSpace(remark.FieldName) != "" {
			aliasToCode[normalizeHeaderAlias(remark.FieldName)] = remark.FieldCode
		}
		if strings.TrimSpace(remark.ChineseRemark) != "" {
			aliasToCode[normalizeHeaderAlias(remark.ChineseRemark)] = remark.FieldCode
		}
	}

	resolved := make([]string, 0, len(columns))
	for _, col := range columns {
		keys := []string{
			normalizeHeaderAlias(col.Code),
			normalizeHeaderAlias(col.Label),
		}
		matched := ""
		for _, key := range keys {
			if key == "" {
				continue
			}
			if code, ok := aliasToCode[key]; ok {
				matched = code
				break
			}
		}
		if matched == "" {
			return nil, fmt.Errorf("导入字段 %s 与当前表单字段不匹配", col.Label)
		}
		resolved = append(resolved, matched)
	}
	return resolved, nil
}

func datasetColumnCodes(columns []enterpriseColumn) []string {
	out := make([]string, 0, len(columns))
	for _, col := range columns {
		out = append(out, col.Code)
	}
	return out
}

func buildEnterpriseDisplayName(base string, index int, total int) string {
	if total <= 1 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, index+1)
}

func buildEnterpriseFormChecksum(sourceChecksum string, sheetName string, index int) string {
	if strings.TrimSpace(sheetName) == "" {
		return sourceChecksum
	}
	return fmt.Sprintf("%s#%s#%d", sourceChecksum, sanitizeFileToken(sheetName), index+1)
}

func writeEnterpriseUploadFile(originalName string, ext string, fileBytes []byte) (string, string, error) {
	if err := os.MkdirAll(enterpriseUploadDir, 0755); err != nil {
		return "", "", err
	}
	storedFilename := fmt.Sprintf("%d_%s%s", time.Now().UnixNano(), sanitizeFileToken(strings.TrimSuffix(originalName, ext)), ext)
	storedPath := filepath.Join(enterpriseUploadDir, storedFilename)
	if err := os.WriteFile(storedPath, fileBytes, 0644); err != nil {
		return "", "", err
	}
	return storedFilename, storedPath, nil
}

func splitTextChunks(content string) []string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	rawParts := regexp.MustCompile(`\n\s*\n+`).Split(content, -1)
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if len(part) <= 1000 {
			parts = append(parts, part)
			continue
		}

		lines := strings.Split(part, "\n")
		var current strings.Builder
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if current.Len()+len(line)+1 > 1000 && current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			if current.Len() > 0 {
				current.WriteByte('\n')
			}
			current.WriteString(line)
		}
		if current.Len() > 0 {
			parts = append(parts, current.String())
		}
	}
	if len(parts) == 0 {
		return []string{content}
	}
	return parts
}

type docxTextNode struct {
	XMLName xml.Name
	Content string         `xml:",chardata"`
	Nodes   []docxTextNode `xml:",any"`
}

func extractDocxText(raw string) string {
	var node docxTextNode
	if err := xml.Unmarshal([]byte(raw), &node); err != nil {
		return raw
	}

	var parts []string
	var walk func(docxTextNode)
	walk = func(n docxTextNode) {
		if strings.EqualFold(n.XMLName.Local, "t") {
			text := strings.TrimSpace(n.Content)
			if text != "" {
				parts = append(parts, text)
			}
		}
		for _, child := range n.Nodes {
			walk(child)
		}
		if strings.EqualFold(n.XMLName.Local, "p") {
			parts = append(parts, "\n")
		}
	}
	walk(node)

	out := strings.Join(parts, " ")
	out = strings.ReplaceAll(out, " \n ", "\n")
	out = strings.ReplaceAll(out, " \n", "\n")
	out = strings.ReplaceAll(out, "\n ", "\n")
	return strings.TrimSpace(out)
}

func firstNonEmptyRowIndex(records [][]string) int {
	for idx, row := range records {
		if countNonEmptyCells(row) > 0 {
			return idx
		}
	}
	return -1
}

func bestExcelHeaderRowIndex(records [][]string) int {
	bestIndex := -1
	bestScore := -1
	limit := len(records)
	if limit > 10 {
		limit = 10
	}
	for idx := 0; idx < limit; idx++ {
		score := countNonEmptyCells(records[idx])
		if score > bestScore {
			bestScore = score
			bestIndex = idx
		}
	}
	if bestScore <= 0 {
		return -1
	}
	return bestIndex
}

func countNonEmptyRows(records [][]string) int {
	count := 0
	for _, row := range records {
		if countNonEmptyCells(row) > 0 {
			count++
		}
	}
	return count
}

func countNonEmptyCells(row []string) int {
	count := 0
	for _, cell := range row {
		if strings.TrimSpace(cell) != "" {
			count++
		}
	}
	return count
}

func maxRecordWidth(records [][]string) int {
	maxWidth := 0
	for _, row := range records {
		if len(row) > maxWidth {
			maxWidth = len(row)
		}
	}
	return maxWidth
}

func normalizeHeaderAlias(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func sanitizeColumnName(input string) string {
	input = strings.TrimSpace(strings.ToLower(input))
	if input == "" {
		return ""
	}

	var b strings.Builder
	prevUnderscore := false
	for _, ch := range input {
		valid := (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')
		if valid {
			b.WriteRune(ch)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}

	out := strings.Trim(b.String(), "_")
	if out == "" {
		return ""
	}
	if out == "object_id" || out == "modified_time" || out == "raw_json" || out == "created_at" || out == "updated_at" {
		return out + "_field"
	}
	return out
}

func checksumBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func isSupportedEnterpriseExt(ext string) bool {
	switch ext {
	case ".csv", ".txt", ".xlsx", ".xlsm", ".xltx", ".docx", ".pdf", ".doc":
		return true
	default:
		return false
	}
}

func sanitizeFileToken(input string) string {
	input = strings.TrimSpace(strings.ToLower(input))
	if input == "" {
		return "enterprise_file"
	}

	var b strings.Builder
	for _, ch := range input {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			b.WriteRune(ch)
			continue
		}
		b.WriteByte('_')
	}

	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "enterprise_file"
	}
	return out
}

func uniqueEnterpriseSchemaCode(displayName string) string {
	base := sanitizeFileToken(displayName)
	if base == "" {
		base = "enterprise_library"
	}
	return fmt.Sprintf("kb_%s_%d", base, time.Now().UnixNano())
}
