package h3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

type FieldMeta struct {
	Code string `json:"code"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type BizObject struct {
	ObjectID     string                 `json:"ObjectId"`
	ModifiedTime *time.Time             `json:"ModifiedTime"`
	Data         map[string]interface{} `json:"-"`
}

func (c *Client) invoke(ctx context.Context, actionName string, payload map[string]interface{}) (map[string]interface{}, error) {
	reqBody := map[string]interface{}{
		"ActionName":   actionName,
		"EngineCode":   c.engineCode,
		"EngineSecret": c.engineSecret,
		"BizObject":    payload,
	}
	b, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("h3 api status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) LoadBizObjects(ctx context.Context, schemaCode string, page, size int, modifiedAfter *time.Time) ([]BizObject, error) {
	filter := map[string]interface{}{}
	if modifiedAfter != nil {
		filter = map[string]interface{}{
			"Matcher": "And",
			"Conditions": []map[string]interface{}{
				{
					"Field":    "ModifiedTime",
					"Operator": ">",
					"Value":    modifiedAfter.Format(time.RFC3339),
				},
			},
		}
	}
	payload := map[string]interface{}{
		"SchemaCode": schemaCode,
		"Page":       page,
		"Size":       size,
		"Filter":     filter,
	}
	res, err := c.invoke(ctx, "LoadBizObjects", payload)
	if err != nil {
		return nil, err
	}

	raw, _ := res["Data"].([]interface{})
	items := make([]BizObject, 0, len(raw))
	for _, x := range raw {
		m, ok := x.(map[string]interface{})
		if !ok {
			continue
		}
		obj := BizObject{Data: m}
		if v, ok := m["ObjectId"].(string); ok {
			obj.ObjectID = v
		}
		if t, ok := m["ModifiedTime"].(string); ok {
			if parsed, e := time.Parse(time.RFC3339, t); e == nil {
				obj.ModifiedTime = &parsed
			}
		}
		items = append(items, obj)
	}
	return items, nil
}

func (c *Client) GetFormFields(ctx context.Context, schemaCode string) ([]FieldMeta, error) {
	payload := map[string]interface{}{"SchemaCode": schemaCode}
	res, err := c.invoke(ctx, "GetMetadata", payload)
	if err != nil {
		return nil, err
	}

	list, _ := res["Fields"].([]interface{})
	fields := make([]FieldMeta, 0, len(list))
	for _, x := range list {
		m, ok := x.(map[string]interface{})
		if !ok {
			continue
		}
		fields = append(fields, FieldMeta{
			Code: asString(m["Code"]),
			Name: asString(m["Name"]),
			Type: asString(m["Type"]),
		})
	}
	return fields, nil
}

func asString(v interface{}) string {
	s, _ := v.(string)
	return s
}

