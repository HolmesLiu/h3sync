package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
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
	Cursor  string        `json:"cursor"`
	Filters []QueryFilter `json:"filters"`
}

type QueryCursor struct {
	ModifiedTime *time.Time `json:"modifiedTime,omitempty"`
	ObjectID     string     `json:"objectId"`
}

type QueryService struct {
	formRepo *repository.FormRepo
}

func NewQueryService(formRepo *repository.FormRepo) *QueryService {
	return &QueryService{formRepo: formRepo}
}

func (s *QueryService) Query(schemaCode string, req QueryRequest) (map[string]interface{}, error) {
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
	fetchLimit := limit + 1
	var rows []map[string]interface{}
	if strings.TrimSpace(req.Cursor) != "" {
		cursor, err := decodeQueryCursor(req.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor")
		}
		rows, err = s.formRepo.QueryRowsByCursor(schemaCode, where, args, fetchLimit, cursor.ModifiedTime, cursor.ObjectID)
		if err != nil {
			return nil, err
		}
	} else {
		rows, err = s.formRepo.QueryRows(schemaCode, where, args, fetchLimit, req.Offset)
		if err != nil {
			return nil, err
		}
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	nextCursor := ""
	if hasMore && len(rows) > 0 {
		nextCursor, err = encodeQueryCursorFromRow(rows[len(rows)-1])
		if err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"form": map[string]interface{}{
			"schemaCode":    form.SchemaCode,
			"displayName":   form.DisplayName,
			"chineseRemark": form.ChineseRemark,
		},
		"fieldRemarks": remarks,
		"rows":         rows,
		"pagination": map[string]interface{}{
			"limit":      limit,
			"offset":     req.Offset,
			"cursor":     strings.TrimSpace(req.Cursor),
			"hasMore":    hasMore,
			"nextCursor": nextCursor,
		},
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

func buildWhere(filters []QueryFilter) (string, []interface{}, error) {
	if len(filters) == 0 {
		return "", nil, nil
	}
	parts := make([]string, 0, len(filters))
	args := make([]interface{}, 0, len(filters)*2)
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
		case "gt", "gte", "lt", "lte":
			expr, value, err := buildComparisonFilter(f.Field, strings.ToLower(f.Operator), f.Value, argN)
			if err != nil {
				return "", nil, err
			}
			parts = append(parts, expr)
			args = append(args, value)
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

func buildComparisonFilter(field string, operator string, value string, argN int) (string, interface{}, error) {
	sqlOp := map[string]string{
		"gt":  ">",
		"gte": ">=",
		"lt":  "<",
		"lte": "<=",
	}[operator]
	if sqlOp == "" {
		return "", nil, fmt.Errorf("unsupported operator: %s", operator)
	}

	if strings.EqualFold(field, "object_id") {
		if _, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			return fmt.Sprintf("(%s ~ '^[0-9]+$' AND %s::numeric %s $%d::numeric)", field, field, sqlOp, argN), strings.TrimSpace(value), nil
		}
	}
	return fmt.Sprintf("%s %s $%d", field, sqlOp, argN), value, nil
}

func decodeQueryCursor(raw string) (QueryCursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return QueryCursor{}, err
	}
	var cursor QueryCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return QueryCursor{}, err
	}
	if strings.TrimSpace(cursor.ObjectID) == "" {
		return QueryCursor{}, fmt.Errorf("missing object id")
	}
	return cursor, nil
}

func encodeQueryCursorFromRow(row map[string]interface{}) (string, error) {
	objectID := strings.TrimSpace(fmt.Sprint(row["object_id"]))
	if objectID == "" || objectID == "<nil>" {
		return "", fmt.Errorf("missing object_id in row")
	}
	cursor := QueryCursor{ObjectID: objectID}
	if v, ok := row["modified_time"]; ok && v != nil {
		switch tv := v.(type) {
		case time.Time:
			t := tv.UTC()
			cursor.ModifiedTime = &t
		case *time.Time:
			if tv != nil {
				t := tv.UTC()
				cursor.ModifiedTime = &t
			}
		case string:
			if strings.TrimSpace(tv) != "" {
				t, err := time.Parse(time.RFC3339, strings.TrimSpace(tv))
				if err != nil {
					return "", err
				}
				utc := t.UTC()
				cursor.ModifiedTime = &utc
			}
		default:
			return "", fmt.Errorf("unsupported modified_time type %T", v)
		}
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
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

