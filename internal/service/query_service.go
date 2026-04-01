package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/HolmesLiu/h3sync/internal/repository"
)

type QueryFilter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type QueryRequest struct {
	Limit   int           `json:"limit"`
	Offset  int           `json:"offset"`
	Filters []QueryFilter `json:"filters"`
}

type QueryService struct {
	formRepo *repository.FormRepo
}

func NewQueryService(formRepo *repository.FormRepo) *QueryService {
	return &QueryService{formRepo: formRepo}
}

func (s *QueryService) Query(schemaCode string, req QueryRequest) (map[string]any, error) {
	form, err := s.formRepo.GetBySchema(schemaCode)
	if err != nil {
		return nil, err
	}
	remarks, err := s.formRepo.ListFieldRemarks(form.ID)
	if err != nil {
		return nil, err
	}

	where, args, err := buildWhere(req.Filters)
	if err != nil {
		return nil, err
	}

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.formRepo.QueryRows(schemaCode, where, args, limit, req.Offset)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"form": map[string]any{
			"schemaCode": form.SchemaCode,
			"displayName": form.DisplayName,
			"chineseRemark": form.ChineseRemark,
		},
		"fieldRemarks": remarks,
		"rows": rows,
	}, nil
}

func (s *QueryService) AddQueryLog(apiKeyID int64, schemaCode string, req QueryRequest, resultCount int, duration time.Duration, callerIP string) {
	b, _ := json.Marshal(req)
	_ = s.formRepo.AddAPIQueryLog(models.APIQueryLog{
		APIKeyID:     apiKeyID,
		SchemaCode:   schemaCode,
		QueryPayload: string(b),
		ResultCount:  resultCount,
		DurationMS:   int(duration.Milliseconds()),
		CallerIP:     callerIP,
	})
}

func buildWhere(filters []QueryFilter) (string, []any, error) {
	if len(filters) == 0 {
		return "", nil, nil
	}
	parts := make([]string, 0, len(filters))
	args := make([]any, 0, len(filters)*2)
	argN := 1
	for _, f := range filters {
		if !isSafeIdentifier(f.Field) {
			continue
		}
		switch strings.ToLower(f.Operator) {
		case "eq":
			parts = append(parts, fmt.Sprintf("%s = $%d", f.Field, argN))
			args = append(args, f.Value)
			argN++
		case "contains":
			parts = append(parts, fmt.Sprintf("%s ILIKE $%d", f.Field, argN))
			args = append(args, "%"+f.Value+"%")
			argN++
		default:
			return "", nil, fmt.Errorf("unsupported operator: %s", f.Operator)
		}
	}
	return strings.Join(parts, " AND "), args, nil
}

func isSafeIdentifier(v string) bool {
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
