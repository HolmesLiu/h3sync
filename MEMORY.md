# h3sync OpenAPI AI Memory

## 基础信息
- 请求方法: POST
- 路径: /openapi/query/{schema}
- 鉴权头: X-API-Key
- Content-Type: application/json

## 鉴权规则
1. key 必须存在且有效
2. key 不能过期/停用
3. key 必须拥有 schema 对应表单权限

失败码:
- 401 missing X-API-Key
- 403 forbidden
- 400 bad request

## 请求体模板
{
  "limit": 100,
  "offset": 0,
  "filters": [
    {"field":"sample_no","operator":"eq","value":"S20260301001"},
    {"field":"customer_name","operator":"contains","value":"瑞普"}
  ]
}

约束:
- operator: eq | contains
- filters 为 AND 关系
- 建议 limit <= 200, 大结果集使用 offset 分页

## 响应体语义
- form: 表单信息(含 chineseRemark)
- fieldRemarks: 字段中文备注
- rows: 数据行，含 raw_json 原始结构

## AI 查询策略
1. 先读 form.chineseRemark + fieldRemarks 再构造查询
2. 优先 eq 精确匹配，contains 用于模糊检索
3. 结果过大时分页拉取
4. 若 403，优先检查 key 权限和过期状态
