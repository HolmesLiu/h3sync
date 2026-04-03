package admin

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/HolmesLiu/h3sync/internal/middleware"
	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/HolmesLiu/h3sync/internal/repository"
	"github.com/HolmesLiu/h3sync/internal/service"
	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Handlers struct {
	AdminService       *service.AdminService
	FormRepo           *repository.FormRepo
	AgentRepo          *repository.AgentRepo
	SyncService        *service.SyncService
	MSSQLBackupService *service.MSSQLBackupService
	EnterpriseService  *service.EnterpriseLibraryService
	APIKeyService      *service.APIKeyService
	Logger             *zap.Logger
}

type formRowView struct {
	models.FormRegistry
	SyncMethodLabel string
	LastSyncLabel   string
	NextSyncLabel   string
	DataCount       int
}

type formGroupView struct {
	ID    string
	Title string
	Forms []formRowView
}

type syncLogView struct {
	ID            int64
	SchemaCode    string
	DisplayName   string
	Status        string
	TriggerLabel  string
	StatusLabel   string
	StartedLabel  string
	FinishedLabel string
	SyncedCount   int
	ErrorMessage  string
}

type mssqlFormRowView struct {
	models.MSSQLFormListView
	SyncMethodLabel string
	LastSyncLabel   string
	NextSyncLabel   string
	DataCount       int
}

type sortFieldOption struct {
	Value string
	Label string
}

type apiKeyRowView struct {
	models.APIKeyListView
	ExpiresLabel string
	ExpiresInput string
}

const loginCaptchaAnswerKey = "login_captcha_answer"

func RegisterRoutes(r *gin.Engine, h Handlers) {
	r.GET("/admin/login", h.loginPage)
	r.POST("/admin/login", h.loginPost)

	admin := r.Group("/admin")
	admin.Use(middleware.RequireAdminLogin(h.AdminService.IsActiveUsername))
	admin.GET("", h.dashboard)
	admin.GET("/forms", h.formsPage)
	admin.POST("/forms", h.saveForm)
	admin.POST("/forms/groups/rename", h.renameGroup)
	admin.POST("/forms/:schema/sync", h.manualSync)
	admin.POST("/forms/:schema/delete", h.deleteForm)
	admin.GET("/forms/:schema/data", h.formDataPage)
	admin.GET("/forms/:schema/export", h.exportFormDataCSV)
	admin.GET("/mssql/forms", h.mssqlFormsPage)
	admin.POST("/mssql/library", h.saveMSSQLLibrary)
	admin.POST("/mssql/discover", h.discoverMSSQLForms)
	admin.POST("/mssql/forms/:schema/bind", h.bindMSSQLForm)
	admin.POST("/mssql/forms/:schema/sync", h.manualSyncMSSQL)
	admin.POST("/mssql/forms/:schema/delete", h.deleteMSSQLForm)
	admin.GET("/mssql/forms/:schema/data", h.formDataPage)
	admin.GET("/mssql/forms/:schema/export", h.exportFormDataCSV)
	admin.GET("/enterprise/forms", h.enterpriseFormsPage)
	admin.POST("/enterprise/forms/upload", h.uploadEnterpriseForm)
	admin.POST("/enterprise/forms/:schema/update", h.updateEnterpriseForm)
	admin.POST("/enterprise/forms/:schema/delete", h.deleteEnterpriseForm)
	admin.GET("/enterprise/forms/:schema/data", h.enterpriseFormDataPage)
	admin.GET("/enterprise/forms/:schema/export", h.exportFormDataCSV)
	admin.POST("/enterprise/forms/:schema/rows/save", h.saveEnterpriseRow)
	admin.POST("/enterprise/forms/:schema/import", h.importEnterpriseRows)
	admin.POST("/enterprise/forms/:schema/rows/:objectID/delete", h.deleteEnterpriseRow)
	admin.POST("/forms/:schema/fields", h.saveFieldRemark)
	admin.GET("/apikeys", h.apiKeysPage)
	admin.POST("/apikeys", h.createAPIKey)
	admin.POST("/apikeys/:id/update", h.updateAPIKey)
	admin.POST("/apikeys/:id/delete", h.deleteAPIKey)
	admin.GET("/users", h.usersPage)
	admin.POST("/users/create", h.createAdminUser)
	admin.POST("/users/:id/update", h.updateAdminUserStatus)
	admin.POST("/users/:id/password", h.updateAdminUserPassword)
	admin.POST("/users/:id/delete", h.deleteAdminUser)
	admin.POST("/apikeys/export-memory", h.exportAPIKeyMemory)
	admin.GET("/agent-gen", h.agentGenPage)
	admin.POST("/agent-gen/roles", h.createAgentRole)
	admin.POST("/agent-gen/roles/:id/delete", h.deleteAgentRole)
	admin.POST("/logout", h.logout)
}

func (h Handlers) loginPage(c *gin.Context) {
	h.renderLoginPage(c, http.StatusOK, "", "")
}

func (h Handlers) loginPost(c *gin.Context) {
	username := strings.TrimSpace(c.PostForm("username"))
	password := c.PostForm("password")
	captcha := strings.TrimSpace(c.PostForm("captcha"))
	sess := sessions.Default(c)
	expected := strings.TrimSpace(fmt.Sprintf("%v", sess.Get(loginCaptchaAnswerKey)))
	sess.Delete(loginCaptchaAnswerKey)
	_ = sess.Save()
	if expected == "" || !strings.EqualFold(expected, captcha) {
		h.renderLoginPage(c, http.StatusUnauthorized, "验证码错误", username)
		return
	}

	_, err := h.AdminService.Login(username, password)
	if err != nil {
		h.renderLoginPage(c, http.StatusUnauthorized, "用户名或密码错误", username)
		return
	}
	sess.Set("admin_user", username)
	_ = sess.Save()
	c.Redirect(http.StatusFound, "/admin")
}

func (h Handlers) renderLoginPage(c *gin.Context, status int, errMsg string, username string) {
	question, answer := generateLoginCaptcha()
	sess := sessions.Default(c)
	sess.Set(loginCaptchaAnswerKey, answer)
	_ = sess.Save()
	c.HTML(status, "login.tmpl", gin.H{
		"title":           "h3sync admin",
		"error":           errMsg,
		"username":        username,
		"captchaQuestion": question,
	})
}

func generateLoginCaptcha() (string, string) {
	left := randomCaptchaNumber(1, 9)
	right := randomCaptchaNumber(1, 9)
	return fmt.Sprintf("%d + %d = ?", left, right), strconv.Itoa(left + right)
}

func randomCaptchaNumber(min int64, max int64) int {
	if max <= min {
		return int(min)
	}
	n, err := rand.Int(rand.Reader, big.NewInt(max-min+1))
	if err != nil {
		return int(min)
	}
	return int(min + n.Int64())
}

func (h Handlers) dashboard(c *gin.Context) {
	c.HTML(http.StatusOK, "dashboard.tmpl", gin.H{"title": "控制台", "navItem": "dashboard"})
}

func (h Handlers) formsPage(c *gin.Context) {
	forms, err := h.FormRepo.ListFormsBySource("H3")
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	groupNames, _ := h.FormRepo.ListGroupNamesBySource("H3")

	dataCount := map[string]int{}
	for _, f := range forms {
		cnt, err := h.FormRepo.CountBizRowsSafe(f.SchemaCode)
		if err != nil {
			cnt = 0
		}
		dataCount[f.SchemaCode] = cnt
	}

	logPage := parseIntOr(c.Query("log_page"), 1)
	if logPage <= 0 {
		logPage = 1
	}
	logSize := parseIntOr(c.Query("log_size"), 20)
	if logSize <= 0 {
		logSize = 20
	}
	if logSize > 200 {
		logSize = 200
	}
	logStatus := strings.TrimSpace(c.Query("log_status"))
	logTrigger := strings.TrimSpace(c.Query("log_trigger"))
	offset := (logPage - 1) * logSize

	totalLogs, err := h.FormRepo.CountSyncLogsBySource("H3", logStatus, logTrigger)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	logsRaw, err := h.FormRepo.ListSyncLogsBySource("H3", logSize, offset, logStatus, logTrigger)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	logs := make([]syncLogView, 0, len(logsRaw))
	for _, l := range logsRaw {
		finished := "-"
		if l.FinishedAt != nil {
			finished = formatMinute(l.FinishedAt)
		}
		errMsg := ""
		if l.ErrorMessage != nil {
			errMsg = *l.ErrorMessage
		}
		logs = append(logs, syncLogView{
			ID:            l.ID,
			SchemaCode:    l.SchemaCode,
			DisplayName:   l.DisplayName,
			Status:        strings.ToUpper(strings.TrimSpace(l.Status)),
			TriggerLabel:  triggerLabel(l.TriggerType),
			StatusLabel:   statusLabel(l.Status),
			StartedLabel:  formatMinute(&l.StartedAt),
			FinishedLabel: finished,
			SyncedCount:   l.SyncedCount,
			ErrorMessage:  errMsg,
		})
	}

	totalPages := totalLogs / logSize
	if totalLogs%logSize != 0 {
		totalPages++
	}
	if totalPages == 0 {
		totalPages = 1
	}
	logPages := buildPages(logPage, totalPages)

	groups := makeGroups(forms, dataCount)
	focus := c.Query("focus")
	if focus == "" && len(groups) > 0 {
		focus = groups[0].ID
	}

	c.HTML(http.StatusOK, "forms.tmpl", gin.H{
		"navItem":       "forms",
		"groups":        groups,
		"logs":          logs,
		"message":       c.Query("msg"),
		"focus":         focus,
		"groupNames":    groupNames,
		"logPage":       logPage,
		"logSize":       logSize,
		"logStatus":     logStatus,
		"logTrigger":    logTrigger,
		"logTotal":      totalLogs,
		"logTotalPages": totalPages,
		"logPages":      logPages,
		"host":          c.Request.Host,
	})
}

func (h Handlers) saveForm(c *gin.Context) {
	interval, _ := strconv.Atoi(c.PostForm("sync_interval_minutes"))
	if interval <= 0 {
		interval = 30
	}
	groupName := strings.TrimSpace(c.PostForm("group_name"))
	newGroup := strings.TrimSpace(c.PostForm("new_group_name"))
	if newGroup != "" {
		groupName = newGroup
	}
	if groupName == "" {
		groupName = "默认分组"
	}

	form := models.FormRegistry{
		SchemaCode:          strings.TrimSpace(c.PostForm("schema_code")),
		SourceType:          "H3",
		GroupName:           groupName,
		DisplayName:         strings.TrimSpace(c.PostForm("display_name")),
		ChineseRemark:       strings.TrimSpace(c.PostForm("chinese_remark")),
		SyncMethod:          strings.ToUpper(strings.TrimSpace(c.PostForm("sync_method"))),
		SyncIntervalMinutes: interval,
		SyncMode:            strings.ToUpper(strings.TrimSpace(c.PostForm("sync_mode"))),
		IsEnabled:           c.PostForm("is_enabled") == "on",
	}
	if form.SyncMethod == "" {
		form.SyncMethod = "AUTO"
	}
	if form.SyncMode == "" {
		form.SyncMode = "INCREMENTAL"
	}
	if err := h.FormRepo.Upsert(form); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	h.audit(c, "FORM_UPSERT", "form", form.SchemaCode, "update form config")
	c.Redirect(http.StatusFound, "/admin/forms?msg=表单配置已保存&focus="+groupID(form.GroupName))
}

func (h Handlers) renameGroup(c *gin.Context) {
	oldName := strings.TrimSpace(c.PostForm("old_group_name"))
	newName := strings.TrimSpace(c.PostForm("new_group_name"))
	sourceType := strings.TrimSpace(c.PostForm("source_type"))
	if sourceType == "" {
		sourceType = "H3"
	}
	if oldName == "" || newName == "" {
		c.Redirect(http.StatusFound, "/admin/forms?msg=分组名称不能为空")
		return
	}
	if err := h.FormRepo.RenameGroupBySource(oldName, newName, sourceType); err != nil {
		if sourceType == "MSSQL_BACKUP" {
			c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=分组重命名失败")
			return
		}
		c.Redirect(http.StatusFound, "/admin/forms?msg=分组重命名失败")
		return
	}
	h.audit(c, "GROUP_RENAME", "group", oldName, "rename group to "+newName)
	if sourceType == "MSSQL_BACKUP" {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=分组重命名成功&focus="+groupID(newName))
		return
	}
	c.Redirect(http.StatusFound, "/admin/forms?msg=分组重命名成功&focus="+groupID(newName))
}

func (h Handlers) manualSync(c *gin.Context) {
	schema := c.Param("schema")
	form, err := h.FormRepo.GetBySchema(schema)
	if err != nil {
		h.Logger.Error("get form failed", zap.String("schema", schema), zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/forms?msg=表单不存在")
		return
	}
	focus := groupID(form.GroupName)
	runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := h.SyncService.SyncBySchema(runCtx, schema, "MANUAL"); err != nil {
		h.Logger.Error("manual sync failed", zap.String("schema", schema), zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/forms?msg=同步失败，请查看日志&focus="+focus)
		return
	}
	h.audit(c, "FORM_SYNC_MANUAL", "form", schema, "trigger manual sync")
	c.Redirect(http.StatusFound, "/admin/forms?msg=手动同步已触发&focus="+focus)
}

func (h Handlers) deleteForm(c *gin.Context) {
	schema := c.Param("schema")
	if err := h.FormRepo.DeleteBySchema(schema); err != nil {
		h.Logger.Error("delete form failed", zap.String("schema", schema), zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/forms?msg=同步失败，请查看日志")
		return
	}
	h.audit(c, "FORM_DELETE", "form", schema, "delete form config and synced data")
	c.Redirect(http.StatusFound, "/admin/forms?msg=表单已删除")
}

func (h Handlers) mssqlFormsPage(c *gin.Context) {
	rootPath, _ := h.FormRepo.GetSystemSetting("mssql_backup_root_path")
	forms, err := h.FormRepo.ListMSSQLForms()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	groupNames, _ := h.FormRepo.ListGroupNamesBySource("MSSQL_BACKUP")

	rows := make([]mssqlFormRowView, 0, len(forms))
	dataCount := map[string]int{}
	for _, f := range forms {
		cnt, err := h.FormRepo.CountBizRowsSafe(f.SchemaCode)
		if err != nil {
			cnt = 0
		}
		dataCount[f.SchemaCode] = cnt
		fr := models.FormRegistry{
			SchemaCode:          f.SchemaCode,
			SyncMethod:          f.SyncMethod,
			SyncIntervalMinutes: f.SyncIntervalMinutes,
			LastSyncAt:          f.LastSyncAt,
			IsEnabled:           true,
		}
		rows = append(rows, mssqlFormRowView{
			MSSQLFormListView: f,
			SyncMethodLabel:   syncMethodLabel(f.SyncMethod),
			LastSyncLabel:     formatMinute(f.LastSyncAt),
			NextSyncLabel:     nextSyncLabel(fr),
			DataCount:         cnt,
		})
	}

	logPage := parseIntOr(c.Query("log_page"), 1)
	if logPage <= 0 {
		logPage = 1
	}
	logSize := parseIntOr(c.Query("log_size"), 20)
	if logSize <= 0 {
		logSize = 20
	}
	if logSize > 200 {
		logSize = 200
	}
	logStatus := strings.TrimSpace(c.Query("log_status"))
	logTrigger := strings.TrimSpace(c.Query("log_trigger"))
	offset := (logPage - 1) * logSize

	totalLogs, err := h.FormRepo.CountSyncLogsBySource("MSSQL_BACKUP", logStatus, logTrigger)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	logsRaw, err := h.FormRepo.ListSyncLogsBySource("MSSQL_BACKUP", logSize, offset, logStatus, logTrigger)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	logs := make([]syncLogView, 0, len(logsRaw))
	for _, l := range logsRaw {
		finished := "-"
		if l.FinishedAt != nil {
			finished = formatMinute(l.FinishedAt)
		}
		errMsg := ""
		if l.ErrorMessage != nil {
			errMsg = *l.ErrorMessage
		}
		logs = append(logs, syncLogView{
			ID:            l.ID,
			SchemaCode:    l.SchemaCode,
			DisplayName:   l.DisplayName,
			Status:        strings.ToUpper(strings.TrimSpace(l.Status)),
			TriggerLabel:  triggerLabel(l.TriggerType),
			StatusLabel:   statusLabel(l.Status),
			StartedLabel:  formatMinute(&l.StartedAt),
			FinishedLabel: finished,
			SyncedCount:   l.SyncedCount,
			ErrorMessage:  errMsg,
		})
	}
	totalPages := totalLogs / logSize
	if totalLogs%logSize != 0 {
		totalPages++
	}
	if totalPages == 0 {
		totalPages = 1
	}
	logPages := buildPages(logPage, totalPages)

	groups := makeMSSQLGroups(rows)
	focus := c.Query("focus")
	if focus == "" && len(groups) > 0 {
		focus = groups[0].ID
	}

	c.HTML(http.StatusOK, "mssql_forms.tmpl", gin.H{
		"navItem":       "mssql_forms",
		"title":         "MSSQL表单管理",
		"groups":        groups,
		"groupNames":    groupNames,
		"rootPath":      rootPath,
		"message":       c.Query("msg"),
		"libraryAt":     formatMinutePtr(time.Now()),
		"defaultGroup":  "MSSQL默认分组",
		"logs":          logs,
		"focus":         focus,
		"logPage":       logPage,
		"logSize":       logSize,
		"logStatus":     logStatus,
		"logTrigger":    logTrigger,
		"logTotal":      totalLogs,
		"logTotalPages": totalPages,
		"logPages":      logPages,
	})
}

func (h Handlers) saveMSSQLLibrary(c *gin.Context) {
	path := strings.TrimSpace(c.PostForm("root_path"))
	if h.MSSQLBackupService == nil {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=MSSQL服务未启用")
		return
	}
	if err := h.MSSQLBackupService.SetBackupRootPath(path); err != nil {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg="+err.Error())
		return
	}
	h.audit(c, "MSSQL_LIBRARY_SAVE", "system_setting", "mssql_backup_root_path", "update backup root path")
	c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=同步根目录已保存")
}

func (h Handlers) discoverMSSQLForms(c *gin.Context) {
	if h.MSSQLBackupService == nil {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=MSSQL服务未启用")
		return
	}
	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	created, updated, err := h.MSSQLBackupService.DiscoverForms(runCtx)
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg="+err.Error())
		return
	}
	h.audit(c, "MSSQL_DISCOVER", "mssql_form", "", fmt.Sprintf("discover forms created=%d updated=%d", created, updated))
	c.Redirect(http.StatusFound, "/admin/mssql/forms?msg="+fmt.Sprintf("扫描完成：新增%d，更新%d", created, updated))
}

func (h Handlers) bindMSSQLForm(c *gin.Context) {
	schema := c.Param("schema")
	displayName := strings.TrimSpace(c.PostForm("display_name"))
	remark := strings.TrimSpace(c.PostForm("chinese_remark"))
	groupName := strings.TrimSpace(c.PostForm("group_name"))
	if groupName == "" {
		groupName = "MSSQL默认分组"
	}

	// NOTE: 读取同步策略配置，让已有的 Poller 自动拾取 MSSQL 表单
	syncMethod := strings.ToUpper(strings.TrimSpace(c.PostForm("sync_method")))
	if syncMethod != "AUTO" {
		syncMethod = "MANUAL"
	}
	interval, _ := strconv.Atoi(c.PostForm("sync_interval_minutes"))
	if interval <= 0 {
		interval = 60
	}

	form, err := h.FormRepo.GetBySchema(schema)
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=表单不存在")
		return
	}
	if !strings.EqualFold(form.SourceType, "MSSQL_BACKUP") {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=仅允许绑定MSSQL表单")
		return
	}
	if displayName != "" {
		form.DisplayName = displayName
	}
	form.ChineseRemark = remark
	form.GroupName = groupName
	form.SyncMethod = syncMethod
	form.SyncIntervalMinutes = interval
	if err := h.FormRepo.Upsert(form); err != nil {
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=保存失败")
		return
	}
	h.audit(c, "MSSQL_FORM_BIND", "form", schema, fmt.Sprintf("update mssql form: method=%s interval=%d", syncMethod, interval))
	c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=配置已保存")
}

func (h Handlers) manualSyncMSSQL(c *gin.Context) {
	schema := c.Param("schema")
	runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := h.SyncService.SyncBySchema(runCtx, schema, "MANUAL"); err != nil {
		h.Logger.Error("manual mssql sync failed", zap.String("schema", schema), zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=同步失败，请查看日志")
		return
	}
	h.audit(c, "MSSQL_FORM_SYNC_MANUAL", "form", schema, "trigger manual sync")
	c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=手动同步已触发")
}

func (h Handlers) deleteMSSQLForm(c *gin.Context) {
	schema := c.Param("schema")
	
	// Physically wipe tracked MSSQL discovery sql files first
	_ = h.MSSQLBackupService.DeleteSourceFiles(schema)
	
	if err := h.FormRepo.DeleteBySchema(schema); err != nil {
		h.Logger.Error("delete mssql form failed", zap.String("schema", schema), zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=删除失败，请查看日志")
		return
	}
	h.audit(c, "MSSQL_FORM_DELETE", "mssql_form", schema, "delete mssql form config and synced data")
	c.Redirect(http.StatusFound, "/admin/mssql/forms?msg=表单及源文件已删除")
}

func (h Handlers) formDataPage(c *gin.Context) {
	schema := c.Param("schema")
	form, err := h.FormRepo.GetBySchema(schema)
	if err != nil {
		c.String(http.StatusNotFound, "form not found")
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
	pages := buildPages(page, totalPages)

	c.HTML(http.StatusOK, "form_data.tmpl", gin.H{
		"navItem":    "mssql_forms",
		"form":       form,
		"schema":     schema,
		"sourceType": form.SourceType,
		"backURL":    dataBackURL(form.SourceType),
		"dataBase":   dataBaseURL(form.SourceType, schema),
		"columns":    columns,
		"rows":       rows,
		"keyword":    keyword,
		"field":      field,
		"sortField":  normalizeSortField(sortField, columns),
		"sortOrder":  sortOrder,
		"sortFields": buildSortFields(columns),
		"size":       size,
		"page":       page,
		"hasPrev":    page > 1,
		"prevPage":   page - 1,
		"hasNext":    page*size < total,
		"nextPage":   page + 1,
		"totalCount": total,
		"totalPages": totalPages,
		"pages":      pages,
	})
}

func (h Handlers) exportFormDataCSV(c *gin.Context) {
	schema := c.Param("schema")
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
	total, err := h.FormRepo.CountBizRowsForAdmin(schema, keyword, field)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	rows, err := h.FormRepo.ListBizRowsForAdmin(schema, columns, keyword, field, sortField, sortOrder, total+1, 0)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	filename := fmt.Sprintf("%s_export.csv", schema)
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", "attachment; filename="+filename)

	w := csv.NewWriter(c.Writer)
	header := []string{"object_id", "modified_time"}
	header = append(header, columns...)
	_ = w.Write(header)

	for _, row := range rows {
		record := make([]string, 0, len(header))
		record = append(record, toString(row["object_id"]))
		record = append(record, toString(row["modified_time"]))
		for _, col := range columns {
			record = append(record, toString(row[col]))
		}
		_ = w.Write(record)
	}
	w.Flush()
}

func (h Handlers) saveFieldRemark(c *gin.Context) {
	schema := c.Param("schema")
	form, err := h.FormRepo.GetBySchema(schema)
	if err != nil {
		if err == sql.ErrNoRows {
			c.String(http.StatusNotFound, "form not found")
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	field := models.FormFieldRegistry{
		FormID:        form.ID,
		FieldCode:     c.PostForm("field_code"),
		FieldName:     c.PostForm("field_name"),
		ChineseRemark: c.PostForm("chinese_remark"),
		ShowInAdmin:   c.PostForm("show_in_admin") == "on",
		OriginalType:  c.PostForm("original_type"),
		StorageType:   c.PostForm("storage_type"),
	}
	if err := h.FormRepo.UpsertFieldRemark(form.ID, field); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if strings.EqualFold(form.SourceType, "ENTERPRISE_LIBRARY") {
		h.audit(c, "FIELD_REMARK_UPSERT", "field", field.FieldCode, "update field remark")
		c.Redirect(http.StatusFound, "/admin/enterprise/forms/"+schema+"/data?msg=字段设置已保存")
		return
	}
	h.audit(c, "FIELD_REMARK_UPSERT", "field", field.FieldCode, "update field remark")
	c.Redirect(http.StatusFound, "/admin/forms?msg=字段备注已保存")
}

func (h Handlers) apiKeysPage(c *gin.Context) {
	keysRaw, err := h.FormRepo.ListAPIKeys()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	keys := make([]apiKeyRowView, 0, len(keysRaw))
	for _, k := range keysRaw {
		keys = append(keys, apiKeyRowView{
			APIKeyListView: k,
			ExpiresLabel:   formatDateTime(k.ExpiresAt),
			ExpiresInput:   formatDateTimeLocalInput(k.ExpiresAt),
		})
	}
	forms, _ := h.FormRepo.ListAllForms()
	groups := makeGroups(forms, map[string]int{})
	permMap := map[int64][]string{}
	for _, k := range keysRaw {
		p, _ := h.FormRepo.GetAPIKeyPermissions(k.ID)
		permMap[k.ID] = p
	}
	permJSON, _ := json.Marshal(permMap)

	c.HTML(http.StatusOK, "apikeys.tmpl", gin.H{
		"navItem":    "apikeys",
		"title":      "API Keys",
		"keys":       keys,
		"formGroups": groups,
		"permJSON":   string(permJSON),
		"message":    c.Query("msg"),
		"host":       c.Request.Host,
	})
}

func (h Handlers) createAPIKey(c *gin.Context) {
	name := strings.TrimSpace(c.PostForm("name"))
	remark := strings.TrimSpace(c.PostForm("remark"))
	autoExpire := c.PostForm("auto_expire") == "on"
	expiresAtRaw := strings.TrimSpace(c.PostForm("expires_at"))

	codes := c.PostFormArray("schema_codes")
	if len(codes) == 0 {
		formsRaw := strings.TrimSpace(c.PostForm("schema_codes_csv"))
		for _, s := range strings.Split(formsRaw, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				codes = append(codes, s)
			}
		}
	}

	var exp *time.Time
	if autoExpire && expiresAtRaw != "" {
		if t, err := parseDateTimeLocal(expiresAtRaw); err == nil {
			exp = &t
		}
	}

	plain, err := h.APIKeyService.Create(name, remark, exp, codes)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	h.audit(c, "APIKEY_CREATE", "apikey", name, "create apikey")

	keysRaw, _ := h.FormRepo.ListAPIKeys()
	keys := make([]apiKeyRowView, 0, len(keysRaw))
	for _, k := range keysRaw {
		keys = append(keys, apiKeyRowView{
			APIKeyListView: k,
			ExpiresLabel:   formatDateTime(k.ExpiresAt),
			ExpiresInput:   formatDateTimeLocalInput(k.ExpiresAt),
		})
	}
	forms, _ := h.FormRepo.ListAllForms()
	groups := makeGroups(forms, map[string]int{})
	permMap := map[int64][]string{}
	for _, k := range keysRaw {
		p, _ := h.FormRepo.GetAPIKeyPermissions(k.ID)
		permMap[k.ID] = p
	}
	permJSON, _ := json.Marshal(permMap)
	c.HTML(http.StatusOK, "apikeys.tmpl", gin.H{
		"navItem":    "apikeys",
		"title":      "API Keys",
		"keys":       keys,
		"formGroups": groups,
		"permJSON":   string(permJSON),
		"createdKey": plain,
		"message":    "API Key 创建成功",
		"host":       c.Request.Host,
	})
}

func (h Handlers) updateAPIKey(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.Redirect(http.StatusFound, "/admin/apikeys?msg=参数错误")
		return
	}
	name := strings.TrimSpace(c.PostForm("name"))
	remark := strings.TrimSpace(c.PostForm("remark"))
	autoExpire := c.PostForm("auto_expire") == "on"
	expiresAtRaw := strings.TrimSpace(c.PostForm("expires_at"))
	codes := c.PostFormArray("schema_codes")

	var exp *time.Time
	if autoExpire && expiresAtRaw != "" {
		if t, err := parseDateTimeLocal(expiresAtRaw); err == nil {
			exp = &t
		}
	}
	if err := h.APIKeyService.Update(id, name, remark, exp, codes); err != nil {
		c.Redirect(http.StatusFound, "/admin/apikeys?msg=更新失败")
		return
	}
	h.audit(c, "APIKEY_UPDATE", "apikey", strconv.FormatInt(id, 10), "update apikey")
	c.Redirect(http.StatusFound, "/admin/apikeys?msg=API Key 已更新")
}

func (h Handlers) deleteAPIKey(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.Redirect(http.StatusFound, "/admin/apikeys?msg=参数错误")
		return
	}
	if err := h.APIKeyService.Delete(id); err != nil {
		c.Redirect(http.StatusFound, "/admin/apikeys?msg=删除失败")
		return
	}
	h.audit(c, "APIKEY_DELETE", "apikey", strconv.FormatInt(id, 10), "delete apikey")
	c.Redirect(http.StatusFound, "/admin/apikeys?msg=API Key 已删除")
}

type memoryExportReq struct {
	Content string `json:"content"`
}

func (h Handlers) exportAPIKeyMemory(c *gin.Context) {
	var req memoryExportReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "bad request"})
		return
	}
	content := strings.TrimSpace(req.Content)
	if content == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "empty content"})
		return
	}
	path := strings.TrimSpace(os.Getenv("OPENCLAW_MEMORY_FILE"))
	if path == "" {
		path = "MEMORY.md"
	}
	if err := os.WriteFile(path, []byte(content+"\n"), 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}
	h.audit(c, "APIKEY_EXPORT_MEMORY", "file", path, "write ai memory template")
	c.JSON(http.StatusOK, gin.H{"message": "ok", "path": path})
}
func (h Handlers) logout(c *gin.Context) {
	sess := sessions.Default(c)
	sess.Clear()
	_ = sess.Save()
	c.Redirect(http.StatusFound, "/admin/login")
}

func (h Handlers) audit(c *gin.Context, action, targetType, targetID, detail string) {
	user := "unknown"
	if v, ok := c.Get("admin_user"); ok {
		user = v.(string)
	}
	h.AdminService.Audit(models.AdminAuditLog{
		Username:   user,
		Action:     action,
		TargetType: targetType,
		TargetID:   targetID,
		Detail:     detail,
		ClientIP:   c.ClientIP(),
	})
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

func makeGroups(forms []models.FormRegistry, dataCount map[string]int) []formGroupView {
	groupsMap := map[string][]formRowView{}
	for _, f := range forms {
		g := strings.TrimSpace(f.GroupName)
		if g == "" {
			g = "默认分组"
		}
		groupsMap[g] = append(groupsMap[g], formRowView{
			FormRegistry:    f,
			SyncMethodLabel: syncMethodLabel(f.SyncMethod),
			LastSyncLabel:   formatMinute(f.LastSyncAt),
			NextSyncLabel:   nextSyncLabel(f),
			DataCount:       dataCount[f.SchemaCode],
		})
	}

	ordered := make([]string, 0, len(groupsMap))
	for k := range groupsMap {
		ordered = append(ordered, k)
	}
	for i := 0; i < len(ordered)-1; i++ {
		for j := i + 1; j < len(ordered); j++ {
			if ordered[i] > ordered[j] {
				ordered[i], ordered[j] = ordered[j], ordered[i]
			}
		}
	}

	groups := make([]formGroupView, 0, len(ordered))
	for _, g := range ordered {
		groups = append(groups, formGroupView{ID: groupID(g), Title: g, Forms: groupsMap[g]})
	}
	return groups
}

func syncMethodLabel(method string) string {
	if strings.EqualFold(method, "MANUAL") {
		return "手动"
	}
	return "自动"
}

func triggerLabel(trigger string) string {
	if strings.EqualFold(trigger, "MANUAL") {
		return "🖐 手动触发"
	}
	return "⏰ 自动触发"
}

func statusLabel(status string) string {
	s := strings.ToUpper(strings.TrimSpace(status))
	switch s {
	case "SUCCESS":
		return "✔ 成功"
	case "FAILED":
		return "✖ 失败"
	case "RUNNING":
		return "⏳ 运行中"
	default:
		return "ℹ " + status
	}
}

func nextSyncLabel(f models.FormRegistry) string {
	if !f.IsEnabled {
		return "停用"
	}
	if strings.EqualFold(f.SyncMethod, "MANUAL") {
		return "手动触发"
	}
	if f.LastSyncAt == nil {
		return "可立即执行"
	}
	n := f.LastSyncAt.Add(time.Duration(f.SyncIntervalMinutes) * time.Minute)
	return n.Format("2006-01-02 15:04")
}

func formatMinute(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04")
}

func formatMinutePtr(t time.Time) string {
	return t.Local().Format("2006-01-02 15:04")
}

func formatDateTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func formatDateTimeLocalInput(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Local().Format("2006-01-02T15:04:05")
}

func parseDateTimeLocal(v string) (time.Time, error) {
	s := strings.TrimSpace(v)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty datetime")
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02T15:04", s)
}

func dataBackURL(sourceType string) string {
	if strings.EqualFold(sourceType, "MSSQL_BACKUP") {
		return "/admin/mssql/forms"
	}
	return "/admin/forms"
}

func dataBaseURL(sourceType string, schema string) string {
	if strings.EqualFold(sourceType, "MSSQL_BACKUP") {
		return "/admin/mssql/forms/" + schema
	}
	return "/admin/forms/" + schema
}

func groupID(group string) string {
	v := strings.TrimSpace(group)
	if v == "" {
		v = "默认分组"
	}
	v = strings.ToLower(v)
	v = strings.ReplaceAll(v, " ", "-")
	v = strings.ReplaceAll(v, "/", "-")
	return "group-" + v
}

type mssqlGroupView struct {
	ID    string
	Title string
	Forms []mssqlFormRowView
}

func makeMSSQLGroups(forms []mssqlFormRowView) []mssqlGroupView {
	m := map[string][]mssqlFormRowView{}
	for _, f := range forms {
		grp := f.GroupName
		if grp == "" {
			grp = "MSSQL默认分组"
		}
		m[grp] = append(m[grp], f)
	}
	titles := make([]string, 0, len(m))
	for k := range m {
		titles = append(titles, k)
	}
	sort.Strings(titles)
	res := make([]mssqlGroupView, 0, len(titles))
	for _, t := range titles {
		res = append(res, mssqlGroupView{
			ID:    groupID(t),
			Title: t,
			Forms: m[t],
		})
	}
	return res
}

func parseIntOr(v string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return n
}

func buildPages(current int, total int) []int {
	if total <= 0 {
		return []int{1}
	}
	start := current - 2
	if start < 1 {
		start = 1
	}
	end := start + 4
	if end > total {
		end = total
	}
	if end-start < 4 {
		start = end - 4
		if start < 1 {
			start = 1
		}
	}
	out := make([]int, 0, 5)
	for i := start; i <= end; i++ {
		out = append(out, i)
	}
	return out
}

func orderDetailColumns(columns []string) []string {
	type scored struct {
		name  string
		score int
	}
	items := make([]scored, 0, len(columns))
	for _, c := range columns {
		l := strings.ToLower(strings.TrimSpace(c))
		score := 100
		switch {
		case l == "id":
			score = 0
		case strings.HasSuffix(l, "_id") || strings.Contains(l, "id"):
			score = 10
		case strings.Contains(l, "time") || strings.Contains(l, "date"):
			score = 20
		}
		items = append(items, scored{name: c, score: score})
	}
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].score > items[j].score || (items[i].score == items[j].score && strings.ToLower(items[i].name) > strings.ToLower(items[j].name)) {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.name)
	}
	return out
}

func buildSortFields(columns []string) []sortFieldOption {
	out := []sortFieldOption{
		{Value: "modified_time", Label: "modified_time"},
		{Value: "object_id", Label: "object_id"},
	}
	for _, c := range columns {
		if isSafeSortField(c) {
			out = append(out, sortFieldOption{Value: c, Label: c})
		}
	}
	return out
}

func normalizeSortField(sortField string, columns []string) string {
	s := strings.TrimSpace(sortField)
	if s == "" {
		return "modified_time"
	}
	if strings.EqualFold(s, "modified_time") || strings.EqualFold(s, "object_id") {
		return strings.ToLower(s)
	}
	for _, c := range columns {
		if strings.EqualFold(c, s) {
			return c
		}
	}
	return "modified_time"
}

func isSafeSortField(v string) bool {
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

func (h Handlers) usersPage(c *gin.Context) {
	users, err := h.AdminService.ListAdminUsers()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	
	page := parseIntOr(c.Query("page"), 1)
	if page <= 0 { page = 1 }
	size := parseIntOr(c.Query("size"), 20)
	if size <= 0 { size = 20 }
	
	action := strings.TrimSpace(c.Query("log_action"))
	username := strings.TrimSpace(c.Query("log_username"))
	
	logs, totalLogs, err := h.AdminService.ListAuditLogs(page, size, action, username)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	
	totalPages := totalLogs / size
	if totalLogs%size != 0 { totalPages++ }
	if totalPages == 0 { totalPages = 1 }
	pages := buildPages(page, totalPages)
	
	c.HTML(http.StatusOK, "users.tmpl", gin.H{
		"title": "用户管理",
		"users": users,
		"logs": logs,
		"page": page,
		"size": size,
		"total": totalLogs,
		"totalPages": totalPages,
		"pages": pages,
		"logAction": action,
		"logUsername": username,
		"message": c.Query("msg"),
		"navItem": "users",
	})
}

func (h Handlers) createAdminUser(c *gin.Context) {
	username := strings.TrimSpace(c.PostForm("username"))
	password := strings.TrimSpace(c.PostForm("password"))
	
	if username == "" || password == "" {
		c.Redirect(http.StatusFound, "/admin/users?msg=用户名和密码不能为空")
		return
	}
	
	err := h.AdminService.CreateAdminUser(username, password)
	if err != nil {
		h.Logger.Error("create user failed", zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/users?msg=创建用户失败，用户名可能已存在")
		return
	}
	
	h.audit(c, "USER_CREATE", "user", username, "created new admin user")
	c.Redirect(http.StatusFound, "/admin/users?msg=用户创建成功")
}

func (h Handlers) updateAdminUserStatus(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.Redirect(http.StatusFound, "/admin/users?msg=参数错误")
		return
	}
	
	isActive := c.PostForm("is_active") == "on"
	err = h.AdminService.UpdateAdminUserStatus(id, isActive)
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/users?msg=状态更新失败")
		return
	}
	
	h.audit(c, "USER_UPDATE", "user", strconv.FormatInt(id, 10), fmt.Sprintf("updated user active status to %v", isActive))
	c.Redirect(http.StatusFound, "/admin/users?msg=用户状态已更新")
}

func (h Handlers) updateAdminUserPassword(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.Redirect(http.StatusFound, "/admin/users?msg=参数错误")
		return
	}
	
	password := strings.TrimSpace(c.PostForm("password"))
	if password == "" {
		c.Redirect(http.StatusFound, "/admin/users?msg=密码不能为空")
		return
	}
	
	err = h.AdminService.UpdateAdminUserPassword(id, password)
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/users?msg=密码更新失败")
		return
	}
	
	h.audit(c, "USER_UPDATE", "user", strconv.FormatInt(id, 10), "reset user password")
	c.Redirect(http.StatusFound, "/admin/users?msg=密码重置成功")
}

func (h Handlers) deleteAdminUser(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.Redirect(http.StatusFound, "/admin/users?msg=参数错误")
		return
	}
	
	err = h.AdminService.DeleteAdminUser(id)
	if err != nil {
		c.Redirect(http.StatusFound, "/admin/users?msg=删除用户失败")
		return
	}
	
	h.audit(c, "USER_DELETE", "user", strconv.FormatInt(id, 10), "deleted user")
	c.Redirect(http.StatusFound, "/admin/users?msg=用户已删除")
}

