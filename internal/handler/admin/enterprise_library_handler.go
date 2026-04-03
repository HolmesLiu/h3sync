package admin

import (
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/HolmesLiu/h3sync/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type enterpriseFormRowView struct {
	models.EnterpriseLibraryListView
	LastSyncLabel string
	ParsedLabel   string
	DataCount     int
	StatusLabel   string
	DisplayTitle  string
}

type enterpriseGroupView struct {
	ID    string
	Title string
	Forms []enterpriseFormRowView
}

type enterpriseFieldView struct {
	Code          string
	Label         string
	ChineseRemark string
	ShowInAdmin   bool
}

type enterpriseEditRowView struct {
	ObjectID     string
	ModifiedTime string
	Fields       map[string]string
	FieldsJSON   string
}

func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (h Handlers) enterpriseFormsPage(c *gin.Context) {
	forms, err := h.FormRepo.ListEnterpriseLibraryForms()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	groupNames, _ := h.FormRepo.ListGroupNamesBySource("ENTERPRISE_LIBRARY")
	rows := make([]enterpriseFormRowView, 0, len(forms))
	for _, f := range forms {
		cnt, err := h.FormRepo.CountBizRowsSafe(f.SchemaCode)
		if err != nil {
			cnt = 0
		}
		title := f.DisplayName
		if strings.TrimSpace(f.SheetName) != "" {
			title = fmt.Sprintf("%s [%s]", f.DisplayName, f.SheetName)
		}
		rows = append(rows, enterpriseFormRowView{
			EnterpriseLibraryListView: f,
			LastSyncLabel:             formatMinute(f.LastSyncAt),
			ParsedLabel:               formatMinute(f.ParsedAt),
			DataCount:                 cnt,
			StatusLabel:               enterpriseStatusLabel(f.ParseStatus),
			DisplayTitle:              title,
		})
	}

	focus := c.Query("focus")
	groups := makeEnterpriseGroups(rows)
	if focus == "" && len(groups) > 0 {
		focus = groups[0].ID
	}

	c.HTML(http.StatusOK, "enterprise_library.tmpl", gin.H{
		"navItem":    "enterprise_forms",
		"title":      "企业资料库",
		"message":    c.Query("msg"),
		"groups":     groups,
		"groupNames": groupNames,
		"focus":      focus,
	})
}

func (h Handlers) uploadEnterpriseForm(c *gin.Context) {
	if h.EnterpriseService == nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=企业资料库服务未启用")
		return
	}
	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=请选择上传文件")
		return
	}
	result, err := h.EnterpriseService.UploadAndImport(enterpriseUploadInputFromContext(c, fileHeader))
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg="+err.Error())
		return
	}
	h.audit(c, "ENTERPRISE_FORM_UPLOAD", "enterprise_form", strings.Join(result.CreatedSchemas, ","), "upload enterprise library file")
	c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg="+fmt.Sprintf("资料库导入完成，共创建 %d 个表单", len(result.CreatedSchemas)))
}

func (h Handlers) updateEnterpriseForm(c *gin.Context) {
	schema := c.Param("schema")
	form, err := h.FormRepo.GetBySchema(schema)
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=资料表不存在")
		return
	}
	if !strings.EqualFold(form.SourceType, "ENTERPRISE_LIBRARY") {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=仅允许编辑企业资料库表单")
		return
	}
	form.DisplayName = strings.TrimSpace(c.PostForm("display_name"))
	form.ChineseRemark = strings.TrimSpace(c.PostForm("chinese_remark"))
	groupName := strings.TrimSpace(c.PostForm("group_name"))
	if groupName == "" {
		groupName = "企业资料库"
	}
	form.GroupName = groupName
	if form.DisplayName == "" {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=表单名称不能为空")
		return
	}
	if err := h.FormRepo.Upsert(form); err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=保存失败")
		return
	}
	h.audit(c, "ENTERPRISE_FORM_UPDATE", "enterprise_form", schema, "update enterprise form metadata")
	c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=资料表已更新&focus="+groupID(groupName))
}

func (h Handlers) deleteEnterpriseForm(c *gin.Context) {
	schema := c.Param("schema")
	meta, _ := h.FormRepo.GetEnterpriseLibraryMetaBySchema(schema)
	if err := h.FormRepo.DeleteBySchema(schema); err != nil {
		h.Logger.Error("delete enterprise form failed", zap.String("schema", schema), zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=删除失败")
		return
	}
	if meta.FilePath != "" {
		if refCount, err := h.FormRepo.EnterpriseLibraryFileReferenceCount(meta.FilePath); err == nil && refCount == 0 {
			_ = os.Remove(meta.FilePath)
		}
	}
	h.audit(c, "ENTERPRISE_FORM_DELETE", "enterprise_form", schema, "delete enterprise library form")
	c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=资料表已删除")
}

func (h Handlers) enterpriseFormDataPage(c *gin.Context) {
	schema := c.Param("schema")
	form, err := h.FormRepo.GetBySchema(schema)
	if err != nil {
		c.String(http.StatusNotFound, "form not found")
		return
	}
	meta, err := h.FormRepo.GetEnterpriseLibraryMetaBySchema(schema)
	if err != nil {
		c.String(http.StatusNotFound, "enterprise metadata not found")
		return
	}

	page := parseIntOr(c.Query("page"), 1)
	if page <= 0 {
		page = 1
	}
	size := parseIntOr(c.Query("size"), 30)
	if size <= 0 {
		size = 30
	}
	if size > 200 {
		size = 200
	}
	offset := (page - 1) * size

	keyword := strings.TrimSpace(c.Query("keyword"))
	field := strings.TrimSpace(c.Query("field"))
	sortField := strings.TrimSpace(c.Query("sort_field"))
	sortOrder := strings.ToUpper(strings.TrimSpace(c.Query("sort_order")))
	if sortOrder != "ASC" {
		sortOrder = "DESC"
	}

	columns, err := h.FormRepo.ListBizColumns(schema)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	columns = orderDetailColumns(columns)
	fieldViews, fieldMap := h.buildEnterpriseFieldViews(form.ID, columns)

	rows, err := h.FormRepo.ListBizRowsForAdmin(schema, columns, keyword, field, sortField, sortOrder, size, offset)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	total, err := h.FormRepo.CountBizRowsForAdmin(schema, keyword, field)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	totalPages := total / size
	if total%size != 0 {
		totalPages++
	}
	if totalPages == 0 {
		totalPages = 1
	}

	editRows := make([]enterpriseEditRowView, 0, len(rows))
	for _, row := range rows {
		fields := map[string]string{}
		for _, col := range columns {
			fields[col] = toString(row[col])
		}
		editRows = append(editRows, enterpriseEditRowView{
			ObjectID:     toString(row["object_id"]),
			ModifiedTime: toString(row["modified_time"]),
			Fields:       fields,
			FieldsJSON:   mustJSON(fields),
		})
	}

	c.HTML(http.StatusOK, "enterprise_library_data.tmpl", gin.H{
		"navItem":     "enterprise_forms",
		"form":        form,
		"meta":        meta,
		"schema":      schema,
		"columns":     columns,
		"fieldViews":  fieldViews,
		"fieldMap":    fieldMap,
		"editRows":    editRows,
		"keyword":     keyword,
		"field":       field,
		"sortField":   normalizeSortField(sortField, columns),
		"sortOrder":   sortOrder,
		"sortFields":  buildSortFields(columns),
		"size":        size,
		"page":        page,
		"hasPrev":     page > 1,
		"prevPage":    page - 1,
		"hasNext":     page*size < total,
		"nextPage":    page + 1,
		"totalCount":  total,
		"totalPages":  totalPages,
		"pages":       buildPages(page, totalPages),
		"message":     c.Query("msg"),
		"dataBase":    "/admin/enterprise/forms/" + schema,
		"backURL":     "/admin/enterprise/forms",
		"importTypes": ".csv,.xlsx,.xlsm,.xltx",
	})
}

func (h Handlers) buildEnterpriseFieldViews(formID int64, columns []string) ([]enterpriseFieldView, map[string]enterpriseFieldView) {
	fieldRemarks, _ := h.FormRepo.ListFieldRemarks(formID)
	remarkMap := map[string]models.FormFieldRegistry{}
	for _, item := range fieldRemarks {
		remarkMap[item.FieldCode] = item
	}
	fieldViews := make([]enterpriseFieldView, 0, len(columns))
	fieldMap := map[string]enterpriseFieldView{}
	for _, col := range columns {
		remark := remarkMap[col]
		label := strings.TrimSpace(remark.FieldName)
		if label == "" {
			label = col
		}
		view := enterpriseFieldView{
			Code:          col,
			Label:         label,
			ChineseRemark: remark.ChineseRemark,
			ShowInAdmin:   remark.ShowInAdmin,
		}
		fieldViews = append(fieldViews, view)
		fieldMap[col] = view
	}
	return fieldViews, fieldMap
}

func (h Handlers) saveEnterpriseRow(c *gin.Context) {
	schema := c.Param("schema")
	form, err := h.FormRepo.GetBySchema(schema)
	if err != nil || !strings.EqualFold(form.SourceType, "ENTERPRISE_LIBRARY") {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=资料表不存在")
		return
	}
	columns, err := h.FormRepo.ListBizColumns(schema)
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=读取字段失败")
		return
	}

	objectID := strings.TrimSpace(c.PostForm("object_id"))
	if objectID == "" {
		objectID = fmt.Sprintf("manual_%d", time.Now().UnixNano())
	}
	fields := map[string]string{}
	for _, col := range columns {
		fields[col] = strings.TrimSpace(c.PostForm("field_" + col))
	}
	raw, _ := json.Marshal(fields)
	now := time.Now().UTC()
	if err := h.FormRepo.UpsertBizRow(schema, objectID, &now, string(raw), fields); err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=保存内容失败")
		return
	}
	h.audit(c, "ENTERPRISE_ROW_SAVE", "enterprise_form_row", schema+":"+objectID, "upsert enterprise row")
	c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=内容已保存")
}

func (h Handlers) importEnterpriseRows(c *gin.Context) {
	if h.EnterpriseService == nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms?msg=企业资料库服务未启用")
		return
	}
	schema := c.Param("schema")
	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=请选择导入文件")
		return
	}
	if err := h.EnterpriseService.ImportIntoForm(service.EnterpriseImportInput{
		SchemaCode: schema,
		FileHeader: fileHeader,
	}); err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg="+err.Error())
		return
	}
	h.audit(c, "ENTERPRISE_ROW_IMPORT", "enterprise_form", schema, "import rows into enterprise form")
	c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=数据导入完成")
}

func (h Handlers) deleteEnterpriseRow(c *gin.Context) {
	schema := c.Param("schema")
	objectID := c.Param("objectID")
	if err := h.FormRepo.DeleteBizRow(schema, objectID); err != nil {
		c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=删除内容失败")
		return
	}
	h.audit(c, "ENTERPRISE_ROW_DELETE", "enterprise_form_row", schema+":"+objectID, "delete enterprise row")
	c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=内容已删除")
}

func enterpriseUploadInputFromContext(c *gin.Context, fileHeader *multipart.FileHeader) service.EnterpriseUploadInput {
	return service.EnterpriseUploadInput{
		DisplayName:   strings.TrimSpace(c.PostForm("display_name")),
		ChineseRemark: strings.TrimSpace(c.PostForm("chinese_remark")),
		GroupName:     strings.TrimSpace(c.PostForm("group_name")),
		FileHeader:    fileHeader,
	}
}

func makeEnterpriseGroups(forms []enterpriseFormRowView) []enterpriseGroupView {
	groupsMap := map[string][]enterpriseFormRowView{}
	for _, f := range forms {
		group := strings.TrimSpace(f.GroupName)
		if group == "" {
			group = "企业资料库"
		}
		groupsMap[group] = append(groupsMap[group], f)
	}
	ordered := make([]string, 0, len(groupsMap))
	for key := range groupsMap {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	groups := make([]enterpriseGroupView, 0, len(ordered))
	for _, key := range ordered {
		groups = append(groups, enterpriseGroupView{
			ID:    groupID(key),
			Title: key,
			Forms: groupsMap[key],
		})
	}
	return groups
}

func enterpriseStatusLabel(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "READY":
		return "已完成"
	case "FAILED":
		return "解析失败"
	case "PROCESSING":
		return "解析中"
	default:
		return status
	}
}
