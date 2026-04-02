# h3sync OpenAPI 查询手册（AI记忆版）

本文档用于给 AI 或第三方服务快速接入 `h3sync` 的查询接口，覆盖从鉴权握手到查询交互全过程。

## 1. 基础信息

- OpenAPI 根路径：`/openapi`
- 查询接口：`POST /openapi/query/{schema}`
- 鉴权头：`X-API-Key: <你的apikey>`
- 数据格式：`Content-Type: application/json`

说明：
- `schema` 是后台表单配置中的 `schema_code`。
- API Key 必须拥有该表单权限，否则返回 `403 forbidden`。
- API Key 过期或被停用，同样会鉴权失败。

## 2. 握手与鉴权流程

每次调用都执行以下校验（无需额外登录接口）：

1. 客户端携带 `X-API-Key`。
2. 服务端校验：
- Key 是否存在且哈希匹配。
- Key 是否未过期、未停用。
- Key 是否绑定了该 `schema` 查询权限。
3. 通过后执行查询并记录 `api_query_logs`。

常见返回：
- `401`：缺少 `X-API-Key`
- `403`：Key 无效或没有该表单权限
- `400`：请求体格式错误、操作符不支持等
- `200`：查询成功

## 3. 请求体定义

```json
{
  "limit": 100,
  "offset": 0,
  "filters": [
    { "field": "sample_no", "operator": "eq", "value": "S20260301001" },
    { "field": "customer_name", "operator": "contains", "value": "瑞普" }
  ]
}
```

字段说明：
- `limit`：单次返回条数，`1-500`，超限会回落默认值。
- `offset`：偏移量，用于分页。
- `filters`：过滤条件数组，多个条件为 `AND` 关系。
- `operator` 目前支持：
- `eq`：等于
- `contains`：模糊匹配（ILIKE）

注意：
- `field` 只能是安全字段名（字母/数字/下划线）。
- 不支持 SQL 片段、函数表达式或复杂嵌套。

## 4. 响应体定义

```json
{
  "form": {
    "schemaCode": "xxx",
    "displayName": "送检明细",
    "chineseRemark": "用于记录送检样本信息"
  },
  "fieldRemarks": [
    {
      "fieldCode": "sample_no",
      "fieldName": "样本编号",
      "chineseRemark": "唯一编号",
      "showInAdmin": true
    }
  ],
  "rows": [
    {
      "object_id": "123456",
      "modified_time": "2026-04-01T08:12:00Z",
      "raw_json": {
        "sample_no": "S20260301001",
        "customer_name": "郑州瑞普生物工程有限公司"
      }
    }
  ]
}
```

语义说明：
- `form`：表单元信息，含中文备注，便于 AI 理解业务上下文。
- `fieldRemarks`：字段备注清单，便于 AI 做字段语义映射。
- `rows`：查询结果，默认按 `modified_time DESC, object_id DESC`。

## 5. 查询示例

### 5.1 精确查询

```bash
curl -X POST "http://127.0.0.1:8080/openapi/query/tb_premix_send_detail" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: hk_xxxxxxxxxxxxxxxxx" \
  -d '{
    "limit": 50,
    "offset": 0,
    "filters": [
      {"field":"sample_no","operator":"eq","value":"S20260301001"}
    ]
  }'
```

### 5.2 模糊查询 + 分页

```bash
curl -X POST "http://127.0.0.1:8080/openapi/query/tb_premix_send_detail" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: hk_xxxxxxxxxxxxxxxxx" \
  -d '{
    "limit": 100,
    "offset": 100,
    "filters": [
      {"field":"customer_name","operator":"contains","value":"瑞普"}
    ]
  }'
```

## 6. AI 调用建议（记忆规则）

建议 AI 在会话中遵循以下规则：

1. 先读取 `form.chineseRemark` + `fieldRemarks`，确认字段业务语义再构造查询。
2. 优先使用 `eq` 精确条件，必要时再用 `contains`。
3. 单次请求 `limit` 不超过 `200`，大结果集按 `offset` 分页。
4. 需要追溯原始结构时优先看 `rows[].raw_json`。
5. 当接口返回 `403` 时，不重试同参数，先检查 key 权限是否包含该表单。

## 7. 故障排查

- 报 `missing X-API-Key`：检查请求头是否携带。
- 报 `forbidden`：检查 key 是否过期、停用、无该表单权限。
- 报 `unsupported operator`：仅支持 `eq`、`contains`。
- 查询为空：先去后台确认该表单已同步且有数据，再核对 `schema` 是否写对。

## 8. 推荐给 AI 的最小调用模板

```json
{
  "intent": "先理解字段备注，再按业务关键词查询",
  "http": {
    "method": "POST",
    "url": "http://127.0.0.1:8080/openapi/query/{schema}",
    "headers": {
      "Content-Type": "application/json",
      "X-API-Key": "<apikey>"
    },
    "body": {
      "limit": 100,
      "offset": 0,
      "filters": []
    }
  }
}
```

