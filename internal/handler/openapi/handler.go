package openapi

import (
	"database/sql"
	"net/http"
	"strings"
	"time"

	"github.com/HolmesLiu/h3sync/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Handlers struct {
	APIKeyService *service.APIKeyService
	QueryService  *service.QueryService
	Logger        *zap.Logger
}

func RegisterRoutes(r *gin.Engine, h Handlers) {
	g := r.Group("/openapi")
	g.POST("/query/:schema", h.query)
}

func (h Handlers) query(c *gin.Context) {
	start := time.Now()
	schema := c.Param("schema")
	key := strings.TrimSpace(c.GetHeader("X-API-Key"))
	if key == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "missing X-API-Key"})
		return
	}

	apiKeyID, err := h.APIKeyService.ValidateForForm(key, schema)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusForbidden, gin.H{"message": "forbidden"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	var req service.QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	result, err := h.QueryService.Query(schema, req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	rows := 0
	if list, ok := result["rows"].([]map[string]interface{}); ok {
		rows = len(list)
	}
	h.QueryService.AddQueryLog(apiKeyID, schema, req, rows, time.Since(start), c.ClientIP())
	c.JSON(http.StatusOK, result)
}

