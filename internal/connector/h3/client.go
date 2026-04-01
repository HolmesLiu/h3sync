package h3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL      string
	engineCode   string
	engineSecret string
	httpClient   *http.Client
}

func NewClient(baseURL, engineCode, engineSecret string, timeout time.Duration) *Client {
	return &Client{
		baseURL:      baseURL,
		engineCode:   engineCode,
		engineSecret: engineSecret,
		httpClient:   &http.Client{Timeout: timeout},
	}
}

type BizObject struct {
	ObjectID     string                 `json:"ObjectId"`
	ModifiedTime *time.Time             `json:"ModifiedTime"`
	Data         map[string]interface{} `json:"-"`
}

type openAPIResponse struct {
	Successful   bool                   `json:"Successful"`
	ErrorMessage string                 `json:"ErrorMessage"`
	ReturnData   map[string]interface{} `json:"ReturnData"`
	Data         interface{}            `json:"Data"`
}

func (c *Client) invoke(ctx context.Context, actionName string, payload map[string]interface{}) (map[string]interface{}, error) {
	reqBody := map[string]interface{}{"ActionName": actionName}
	for k, v := range payload {
		reqBody[k] = v
	}
	b, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("EngineCode", c.engineCode)
	req.Header.Set("EngineSecret", c.engineSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("h3 api status: %d", resp.StatusCode)
	}

	var r openAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	if !r.Successful {
		if strings.TrimSpace(r.ErrorMessage) == "" {
			return nil, fmt.Errorf("h3 action %s failed", actionName)
		}
		return nil, fmt.Errorf("h3 action %s failed: %s", actionName, r.ErrorMessage)
	}
	if r.ReturnData != nil {
		return r.ReturnData, nil
	}
	if r.Data != nil {
		if m, ok := r.Data.(map[string]interface{}); ok {
			return m, nil
		}
	}
	return map[string]interface{}{}, nil
}

func (c *Client) LoadBizObjects(ctx context.Context, schemaCode string, page, size int, modifiedAfter *time.Time) ([]BizObject, error) {
	from := (page - 1) * size
	to := page * size
	matcher := map[string]interface{}{"Type": "And", "Matchers": []interface{}{}}
	if modifiedAfter != nil {
		matcher = map[string]interface{}{
			"Type": "And",
			"Matchers": []interface{}{
				map[string]interface{}{
					"Type": "Item",
					"Name": "ModifiedTime",
					"Operator": 0,
					"Value": modifiedAfter.Format("2006/1/2 15:04:05"),
				},
			},
		}
	}
	filterObj := map[string]interface{}{
		"FromRowNum":      from,
		"ToRowNum":        to,
		"RequireCount":    false,
		"ReturnItems":     []interface{}{},
		"SortByCollection": []interface{}{},
		"Matcher":         matcher,
	}
	fb, _ := json.Marshal(filterObj)
	payload := map[string]interface{}{
		"SchemaCode": schemaCode,
		"Filter":     string(fb),
	}

	res, err := c.invoke(ctx, "LoadBizObjects", payload)
	if err != nil {
		return nil, err
	}

	raw := pickArray(res, "BizObjectArray", "Data", "Items", "List")
	items := make([]BizObject, 0, len(raw))
	for _, x := range raw {
		m, ok := x.(map[string]interface{})
		if !ok {
			continue
		}
		obj := BizObject{Data: m}
		obj.ObjectID = firstString(m, "ObjectId", "objectId", "id")
		if t := firstString(m, "ModifiedTime", "modifiedTime", "modified_time"); t != "" {
			if parsed, e := parseTime(t); e == nil {
				obj.ModifiedTime = &parsed
			}
		}
		items = append(items, obj)
	}
	return items, nil
}

func pickArray(m map[string]interface{}, keys ...string) []interface{} {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if arr, ok := v.([]interface{}); ok {
				return arr
			}
		}
	}
	return []interface{}{}
}

func firstString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func parseTime(s string) (time.Time, error) {
	layouts := []string{
		"2006/1/2 15:04:05",
		"2006/01/02 15:04:05",
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, time.Local); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %s", s)
}
