package admin

import (
	"database/sql"
	"net/http"
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
	AdminService  *service.AdminService
	FormRepo      *repository.FormRepo
	SyncService   *service.SyncService
	APIKeyService *service.APIKeyService
	Logger        *zap.Logger
}

func RegisterRoutes(r *gin.Engine, h Handlers) {
	r.GET("/admin/login", h.loginPage)
	r.POST("/admin/login", h.loginPost)

	admin := r.Group("/admin")
	admin.Use(middleware.RequireAdminLogin())
	admin.GET("", h.dashboard)
	admin.GET("/forms", h.formsPage)
	admin.POST("/forms", h.saveForm)
	admin.POST("/forms/:schema/sync", h.manualSync)
	admin.POST("/forms/:schema/fields", h.saveFieldRemark)
	admin.GET("/apikeys", h.apiKeysPage)
	admin.POST("/apikeys", h.createAPIKey)
	admin.POST("/logout", h.logout)
}

func (h Handlers) loginPage(c *gin.Context) {
	c.HTML(http.StatusOK, "login.tmpl", gin.H{"title": "h3sync admin"})
}

func (h Handlers) loginPost(c *gin.Context) {
	username := c.PostForm("username")
	password := c.PostForm("password")
	_, err := h.AdminService.Login(username, password)
	if err != nil {
		c.HTML(http.StatusUnauthorized, "login.tmpl", gin.H{"error": "用户名或密码错误"})
		return
	}
	sess := sessions.Default(c)
	sess.Set("admin_user", username)
	_ = sess.Save()
	c.Redirect(http.StatusFound, "/admin")
}

func (h Handlers) dashboard(c *gin.Context) {
	c.HTML(http.StatusOK, "dashboard.tmpl", gin.H{"title": "控制台"})
}

func (h Handlers) formsPage(c *gin.Context) {
	forms, err := h.FormRepo.ListAllForms()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	logs, err := h.FormRepo.ListRecentSyncLogs(20)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.HTML(http.StatusOK, "forms.tmpl", gin.H{
		"forms":   forms,
		"logs":    logs,
		"message": c.Query("msg"),
	})
}

func (h Handlers) saveForm(c *gin.Context) {
	interval, _ := strconv.Atoi(c.PostForm("sync_interval_minutes"))
	if interval <= 0 {
		interval = 30
	}
	form := models.FormRegistry{
		SchemaCode:          strings.TrimSpace(c.PostForm("schema_code")),
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
	c.Redirect(http.StatusFound, "/admin/forms?msg=表单配置已保存")
}

func (h Handlers) manualSync(c *gin.Context) {
	schema := c.Param("schema")
	if err := h.SyncService.SyncBySchema(c.Request.Context(), schema, "MANUAL"); err != nil {
		h.Logger.Error("manual sync failed", zap.String("schema", schema), zap.Error(err))
		c.Redirect(http.StatusFound, "/admin/forms?msg=同步失败，请查看日志")
		return
	}
	h.audit(c, "FORM_SYNC_MANUAL", "form", schema, "trigger manual sync")
	c.Redirect(http.StatusFound, "/admin/forms?msg=手动同步已触发")
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
	h.audit(c, "FIELD_REMARK_UPSERT", "field", field.FieldCode, "update field remark")
	c.Redirect(http.StatusFound, "/admin/forms?msg=字段备注已保存")
}

func (h Handlers) apiKeysPage(c *gin.Context) {
	c.HTML(http.StatusOK, "apikeys.tmpl", gin.H{"title": "API Keys"})
}

func (h Handlers) createAPIKey(c *gin.Context) {
	name := strings.TrimSpace(c.PostForm("name"))
	remark := strings.TrimSpace(c.PostForm("remark"))
	expiresAtRaw := strings.TrimSpace(c.PostForm("expires_at"))
	formsRaw := strings.TrimSpace(c.PostForm("schema_codes"))
	codes := []string{}
	for _, s := range strings.Split(formsRaw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			codes = append(codes, s)
		}
	}

	var exp *time.Time
	if expiresAtRaw != "" {
		if t, err := time.Parse("2006-01-02", expiresAtRaw); err == nil {
			exp = &t
		}
	}

	plain, err := h.APIKeyService.Create(name, remark, exp, codes)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	h.audit(c, "APIKEY_CREATE", "apikey", name, "create apikey")
	c.HTML(http.StatusOK, "apikeys.tmpl", gin.H{"createdKey": plain})
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
