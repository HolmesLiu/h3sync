# h3sync OpenAPI 查询手册（AI记忆版）

本文档用于给 AI 或第三方服务快速接入 `h3sync` 的查询接口，覆盖从鉴权握手到查询交互全过程。

## 1. 基础信息

- OpenAPI 根路径：`/openapi`
- **步骤一：握手与表单发现**：`GET /openapi/forms` （通过该接口获取你能查询的表单 schema 列表和中文语义）
- **步骤二：查询表单数据**：`POST /openapi/query/{schema}`
- 鉴权头：`X-API-Key: <你的apikey>`
- 数据格式：`Content-Type: application/json`

说明：
- `schema` 是后台表单配置中的 `schema_code`。
- API Key 必须拥有该表单权限，否则返回 `403 forbidden`。
- API Key 过期或被停用，同样会鉴权失败。

## 2. 握手与鉴权流程

每一次全新的对话交互，必须遵循以下步骤：

1. 客户端携带 `X-API-Key` 请求 `GET /openapi/forms` 获取可用表单。
2. 收到返回的表单列表（包含 `schemaCode` / `displayName` / `chineseRemark`）后，根据用户的意图锁定正确的表单。
3. 把选定的 `schemaCode` 拼接入 URL，向 `POST /openapi/query/{schema}` 发送查询请求检索真实数据。
4. 若 API Key 不合法、被停用或没权限，均会返回异常状态码。

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

1. 开场必须先调用 `GET /openapi/forms`，了解当前 Key 授权范围内到底有哪些表单。绝对不要凭空猜测 `schema`。
2. 获取到列表后，利用返回的 `chineseRemark` 理解各个表单的作用，再向 `POST /openapi/query/{schema}` 发起具体数据查询。
3. 阅读 `POST` 接口返回的 `fieldRemarks` 熟悉字段结构。
4. 优先使用 `eq` 精确条件过滤数据，必要时再用 `contains`。
5. 单次请求 `limit` 不超过 `200`，超大数据集注意利用 `offset` 分页读取。
6. 需要获取复杂的原生 JSON 树状结构时，优先读取 `rows[].raw_json`。
7. 遇到 `403` 不重试同参数，说明 Key 权限没包含该表单，应明确告知用户权限受限。

## 7. 故障排查

- 报 `missing X-API-Key`：检查请求头是否携带。
- 报 `forbidden`：检查 key 是否过期、停用、无该表单权限。
- 报 `unsupported operator`：仅支持 `eq`、`contains`。
- 查询为空：先去后台确认该表单已同步且有数据，再核对 `schema` 是否写对。

## 8. 推荐给 AI 的最小调用模板

```json
[
  {
    "intent": "第一步：先通过握手获取授权表单列表及其备注",
    "http": {
      "method": "GET",
      "url": "http://127.0.0.1:8080/openapi/forms",
      "headers": {
        "X-API-Key": "<apikey>"
      }
    }
  },
  {
    "intent": "第二步：在选中正确的 schema 后进行内容检索",
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
]
```

