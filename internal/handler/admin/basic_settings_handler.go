package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const basicSettingsKey = "basic_profile_config"
const basicSettingsUploadDir = "uploads/site_assets"

type basicSettings struct {
	CompanyName     string `json:"company_name"`
	CompanyAddress  string `json:"company_address"`
	CompanyPhone    string `json:"company_phone"`
	CompanyEmail    string `json:"company_email"`
	CompanyWebsite  string `json:"company_website"`
	WorkingHours    string `json:"working_hours"`
	CompanyIntro    string `json:"company_intro"`
	SiteLogoPath    string `json:"site_logo_path"`
	SiteName        string `json:"site_name"`
	SiteDomain      string `json:"site_domain"`
	SiteICP         string `json:"site_icp"`
	SiteCertificate string `json:"site_certificate"`
	SiteEmail       string `json:"site_email"`
}

func (h Handlers) basicSettingsPage(c *gin.Context) {
	cfg, _ := h.loadBasicSettings()
	c.HTML(http.StatusOK, "basic_settings.tmpl", gin.H{
		"navItem": "basic-settings",
		"title":   "基本设置",
		"message": c.Query("msg"),
		"config":  cfg,
	})
}

func (h Handlers) saveBasicSettings(c *gin.Context) {
	cfg, _ := h.loadBasicSettings()
	cfg.CompanyName = strings.TrimSpace(c.PostForm("company_name"))
	cfg.CompanyAddress = strings.TrimSpace(c.PostForm("company_address"))
	cfg.CompanyPhone = strings.TrimSpace(c.PostForm("company_phone"))
	cfg.CompanyEmail = strings.TrimSpace(c.PostForm("company_email"))
	cfg.CompanyWebsite = strings.TrimSpace(c.PostForm("company_website"))
	cfg.WorkingHours = strings.TrimSpace(c.PostForm("working_hours"))
	cfg.CompanyIntro = strings.TrimSpace(c.PostForm("company_intro"))
	cfg.SiteName = strings.TrimSpace(c.PostForm("site_name"))
	cfg.SiteDomain = strings.TrimSpace(c.PostForm("site_domain"))
	cfg.SiteICP = strings.TrimSpace(c.PostForm("site_icp"))
	cfg.SiteCertificate = strings.TrimSpace(c.PostForm("site_certificate"))
	cfg.SiteEmail = strings.TrimSpace(c.PostForm("site_email"))

	fileHeader, err := c.FormFile("site_logo")
	if err == nil && fileHeader != nil && strings.TrimSpace(fileHeader.Filename) != "" {
		logoPath, saveErr := saveBasicLogo(fileHeader)
		if saveErr != nil {
			c.Redirect(http.StatusFound, "/admin/basic-settings?msg="+saveErr.Error())
			return
		}
		cfg.SiteLogoPath = logoPath
	}

	if err := h.persistBasicSettings(cfg); err != nil {
		c.Redirect(http.StatusFound, "/admin/basic-settings?msg=保存失败")
		return
	}

	h.audit(c, "BASIC_SETTINGS_SAVE", "system_setting", basicSettingsKey, "update basic settings")
	c.Redirect(http.StatusFound, "/admin/basic-settings?msg=基本设置已保存")
}

func (h Handlers) siteProfile(c *gin.Context) {
	cfg, _ := h.loadBasicSettings()
	c.JSON(http.StatusOK, gin.H{
		"site_name":        cfg.SiteName,
		"site_logo_path":   cfg.SiteLogoPath,
		"site_domain":      cfg.SiteDomain,
		"site_icp":         cfg.SiteICP,
		"site_certificate": cfg.SiteCertificate,
		"site_email":       cfg.SiteEmail,
	})
}

func (h Handlers) loadBasicSettings() (basicSettings, error) {
	raw, err := h.FormRepo.GetSystemSetting(basicSettingsKey)
	if err != nil {
		return basicSettings{}, err
	}
	if strings.TrimSpace(raw) == "" {
		return basicSettings{}, nil
	}

	var cfg basicSettings
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return basicSettings{}, err
	}
	return cfg, nil
}

func (h Handlers) persistBasicSettings(cfg basicSettings) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return h.FormRepo.SetSystemSetting(basicSettingsKey, string(data))
}

func saveBasicLogo(fileHeader *multipart.FileHeader) (string, error) {
	ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
	switch ext {
	case ".png", ".jpg", ".jpeg", ".webp", ".svg":
	default:
		return "", fmt.Errorf("logo 仅支持 png/jpg/jpeg/webp/svg")
	}

	if err := os.MkdirAll(basicSettingsUploadDir, 0755); err != nil {
		return "", err
	}

	src, err := fileHeader.Open()
	if err != nil {
		return "", err
	}
	defer src.Close()

	filename := fmt.Sprintf("site_logo_%d%s", time.Now().UnixNano(), ext)
	absPath := filepath.Join(basicSettingsUploadDir, filename)
	dst, err := os.Create(absPath)
	if err != nil {
		return "", err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return "", err
	}

	return "/uploads/site_assets/" + filename, nil
}
