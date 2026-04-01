package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv              string
	AppName             string
	ListenAddr          string
	DatabaseURL         string
	SessionSecret       string
	AdminInitUser       string
	AdminInitPassword   string
	H3BaseURL           string
	H3EngineCode        string
	H3EngineSecret      string
	SyncPageSize        int
	SyncPollSeconds     int
	LogDir              string
	ApiDefaultPageLimit int
	RequestTimeout      time.Duration
}

func Load() (Config, error) {
	_ = godotenv.Load()

	cfg := Config{
		AppEnv:              envOrDefault("APP_ENV", "dev"),
		AppName:             envOrDefault("APP_NAME", "h3sync"),
		ListenAddr:          envOrDefault("LISTEN_ADDR", ":8080"),
		DatabaseURL:         os.Getenv("DATABASE_URL"),
		SessionSecret:       envOrDefault("SESSION_SECRET", "change-me-in-prod"),
		AdminInitUser:       envOrDefault("ADMIN_INIT_USER", "admin"),
		AdminInitPassword:   envOrDefault("ADMIN_INIT_PASSWORD", "ChangeMe123!"),
		H3BaseURL:           envOrDefault("H3_BASE_URL", "https://www.h3yun.com/OpenApi/Invoke"),
		H3EngineCode:        os.Getenv("H3_ENGINE_CODE"),
		H3EngineSecret:      os.Getenv("H3_ENGINE_SECRET"),
		SyncPageSize:        intOrDefault("SYNC_PAGE_SIZE", 200),
		SyncPollSeconds:     intOrDefault("SYNC_POLL_SECONDS", 30),
		LogDir:              envOrDefault("LOG_DIR", "./logs"),
		ApiDefaultPageLimit: intOrDefault("API_DEFAULT_PAGE_LIMIT", 100),
		RequestTimeout:      time.Duration(intOrDefault("REQUEST_TIMEOUT_SECONDS", 30)) * time.Second,
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intOrDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
