package app

import (
	"context"
	"net/http"
	"time"

	"github.com/HolmesLiu/h3sync/internal/config"
	"github.com/HolmesLiu/h3sync/internal/connector/h3"
	"github.com/HolmesLiu/h3sync/internal/db"
	"github.com/HolmesLiu/h3sync/internal/handler/admin"
	openapihandler "github.com/HolmesLiu/h3sync/internal/handler/openapi"
	"github.com/HolmesLiu/h3sync/internal/logger"
	"github.com/HolmesLiu/h3sync/internal/repository"
	"github.com/HolmesLiu/h3sync/internal/scheduler"
	"github.com/HolmesLiu/h3sync/internal/service"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
)

func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log, err := logger.New(cfg.LogDir)
	if err != nil {
		return err
	}
	defer log.Sync()

	database, err := db.Open(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer database.Close()

	if err := db.Migrate(database); err != nil {
		return err
	}

	adminRepo := repository.NewAdminUserRepo(database)
	formRepo := repository.NewFormRepo(database)
	adminSvc := service.NewAdminService(adminRepo)
	_ = adminSvc.Bootstrap(cfg.AdminInitUser, cfg.AdminInitPassword)

	h3Client := h3.NewClient(cfg.H3BaseURL, cfg.H3EngineCode, cfg.H3EngineSecret, cfg.RequestTimeout)
	mssqlBackupSvc := service.NewMSSQLBackupService(formRepo, log)
	syncSvc := service.NewSyncService(formRepo, h3Client, mssqlBackupSvc, cfg.SyncPageSize, log)
	apiKeySvc := service.NewAPIKeyService(formRepo)
	querySvc := service.NewQueryService(formRepo)
	agentRepo := repository.NewAgentRepo(database)

	r := gin.Default()
	r.LoadHTMLGlob("web/templates/*")
	r.Static("/static", "web/static")

	store := cookie.NewStore([]byte(cfg.SessionSecret))
	r.Use(sessions.Sessions("h3sync_session", store))

	admin.RegisterRoutes(r, admin.Handlers{
		AdminService:       adminSvc,
		FormRepo:           formRepo,
		AgentRepo:          agentRepo,
		SyncService:        syncSvc,
		MSSQLBackupService: mssqlBackupSvc,
		APIKeyService:      apiKeySvc,
		Logger:             log,
	})
	openapihandler.RegisterRoutes(r, openapihandler.Handlers{
		APIKeyService: apiKeySvc,
		QueryService:  querySvc,
		Logger:        log,
	})

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "time": time.Now().UTC()})
	})

	poller := scheduler.NewPoller(syncSvc, cfg.SyncPollSeconds, log)
	go poller.Start(ctx)

	srv := &http.Server{Addr: cfg.ListenAddr, Handler: r}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	return srv.ListenAndServe()
}
