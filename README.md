# 古籍拓片服务：缺损修补 + 影像入库流水线

纯后端、零第三方依赖的 Node 服务（仅用 Node 内置模块，Node ≥ 20）。
数据持久化在单个 JSON 文件 `data/db.json`，写入采用 **临时文件 + fsync + 原子 rename**，进程被杀不会留下半截数据。

## 启动

```bash
# 默认端口 3020，默认数据文件 data/db.json
node server.js
# 或
npm start
```

环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3020` | 监听端口，`0` 表示由系统分配 |
| `DB_FILE` | `data/db.json` | JSON 数据文件路径 |
| `LEASE_MS` | `30000` | 作业默认租约时长（毫秒），认领时可用 `leaseMs` 覆盖 |
| `MAX_ATTEMPTS` | `3` | 最大投递次数（认领次数，含首次），用尽转死信 |
| `CRASH_SENTINEL` | 无 | 测试用：指向一个文件路径，该文件存在时下一次写事务落盘前进程 `SIGKILL` 自杀 |

健康检查：`GET /health`。

## 测试

```bash
npm test
```

使用内置 `node:test` + 全局 `fetch`，共 17 个用例，含：

- checksum 去重、作业自动创建、状态流转与结果唯一；
- 租约：唯一认领、过期抢占、旧持有者提交被拒；
- 失败重试与死信；按状态/拓片查询与统计；
- **崩溃恢复**：真实子进程 `SIGKILL` 验证“结果与状态一起生效、不留半成品”，
  以及 processing 中崩溃后租约过期可被其他工作者重新认领。

## 影像入库流水线

### 作业状态机

```
                 登记影像（POST /images，checksum 去重）
                          │
                          ▼
                      ┌────────┐  认领 POST /jobs/claim     ┌────────────┐
                      │ queued │ ─────────────────────────▶ │ processing │
                      └────────┘                              └─────┬──────┘
                          ▲                                         │
               失败且次数未用尽（回排队）          ┌─────────────────┼─────────────────┐
               └────────────────────────────────  │                 │                 │
                                                  ▼                 ▼                 ▼
                                             ┌──────────┐    ┌───────────┐    ┌─────────────┐
                              成功 + 一条结果 │completed │    │（租约过期）│    │ dead_letter │
                                             └──────────┘    └───────────┘    └─────────────┘
                                                                  可重新认领      失败次数用尽
```

- 作业状态：`queued`（排队）、`processing`（处理中）、`completed`（已完成）、`dead_letter`（死信）。
- 登记影像即在**同一事务**内自动创建作业；checksum 相同的影像全库只留一份，重复登记返回原记录（HTTP 200，`duplicated: true`），不再建作业。
- 认领带租约：返回 `workerId` 与 `leaseExpiresAt`。租约未过期时，只有持有者能交结果/报失败；
  租约过期后作业可被任意工作者重新认领（`reclaimed: true`，`attempts +1`），**旧持有者的提交一律返回 409**。
- 成功提交在同一事务里写入结果并置 `completed`：同一作业最多一条结果，重复完成返回 409。
- 失败后若 `attempts < maxAttempts` 回 `queued` 等待重试；达到上限转 `dead_letter`，不再被认领。

### 崩溃与重启保证

- 所有状态变更走串行写事务：同一进程内请求不会交错写；
- 先改内存状态，再 `write 临时文件 → fsync → rename` 覆盖正式文件。落盘前被杀，磁盘保持上一个完整版本；
  落盘后被杀，结果与 completed 状态同时可见——不存在“状态完成了但没结果”或反之的半成品；
- 重启后：`queued`/`processing`（视租约）作业不丢、可继续；`completed` 不重跑、结果可查；`dead_letter` 仍是死信。

### 接口（影像流水线）

所有请求/响应均为 JSON。错误统一形如 `{"error": "..."}`。

#### `POST /images` — 登记影像并自动建作业

请求：

```json
{
  "rubbingId": "rubbing_demo",
  "fileName": "TP-清-014_正面.tif",
  "checksum": "sha256:3f1b...",
  "sizeBytes": 12582912,
  "contentType": "image/tiff"
}
```

- `rubbingId`、`fileName`、`checksum` 必填；拓片不存在返回 404。
- 首次登记返回 **201**：`{"duplicated": false, "data": {"image": {...}, "job": {...}}}`。
- checksum 已存在返回 **200**：`{"duplicated": true, "data": {"image": 原影像, "job": 原作业}}`。

#### `GET /images?rubbingId=` — 列影像（可按拓片过滤，附带作业）

#### `GET /images/:id` — 单个影像（附带作业）

#### `POST /jobs/claim` — 工作者认领一个作业

请求：`{"workerId": "worker-a", "leaseMs": 30000}`（`leaseMs` 可选，覆盖默认租约）。

- 有可领作业：`200 {"claimed": true, "reclaimed": false, "data": {作业+image+result}}`；
- 抢占过期作业：`reclaimed: true`；
- 无作业可领：`200 {"claimed": false, "data": null}`。

#### `POST /jobs/:id/complete` — 持有者提交结果

请求：`{"workerId": "worker-a", "output": {...}, "metrics": {"ms": 820}}`。

- 成功：`200`，作业 `completed` 且内附唯一 `result`；
- 非持有者 / 租约已过期 / 已完成：`409`。

#### `POST /jobs/:id/fail` — 持有者报告失败

请求：`{"workerId": "worker-a", "error": "影压失败"}`。

- 可重试：`200 {"retried": true, "data": {status: "queued", ...}}`；
- 次数用尽：`200 {"retried": false, "data": {status: "dead_letter", ...}}`；
- 非持有者或租约过期：`409`。

#### `GET /jobs?status=&rubbingId=&imageId=&limit=` — 查作业

- `status` 只能是四种状态之一，否则 400；条件可组合，按创建时间升序。

#### `GET /jobs/stats?rubbingId=` — 状态数量与死信

```json
{
  "data": {
    "total": 3,
    "counts": { "queued": 1, "processing": 0, "completed": 1, "dead_letter": 1 },
    "deadLetters": [ {完整作业视图} ]
  }
}
```

#### `GET /jobs/:id` — 单个作业（含 `image` 与 `result`）

#### `GET /results/:jobId` — 查作业结果（未完成时 `data` 为 `null`）

### 工作者侧推荐用法

1. 循环 `POST /jobs/claim`，领不到就退避一会儿；
2. 在 `leaseExpiresAt` 之前处理完并提交；处理时间可能超租约时，用更短的任务切片或认领更长的 `leaseMs`；
3. 提交收到 409（租约过期/已被抢占）说明已有别人接手，放弃本地结果即可，**不要重试提交**；
4. 失败调 `/fail` 交回队列或进死信；死信由人工排查（看 `lastError`、`attempts`）。

### 端到端示例

```bash
# 1. 登记
curl -s -X POST http://127.0.0.1:3020/images \
  -H 'Content-Type: application/json' \
  -d '{"rubbingId":"rubbing_demo","fileName":"a.tif","checksum":"sha256:abc123"}'

# 2. 工作者认领
curl -s -X POST http://127.0.0.1:3020/jobs/claim \
  -H 'Content-Type: application/json' -d '{"workerId":"w1"}'

# 3. 提交结果（JOB_ID 用上一步返回的作业 id）
curl -s -X POST http://127.0.0.1:3020/jobs/$JOB_ID/complete \
  -H 'Content-Type: application/json' \
  -d '{"workerId":"w1","output":{"storedAs":"oss://bucket/a.tif"}}'

# 4. 状态数量与死信
curl -s http://127.0.0.1:3020/jobs/stats
```

## 缺损修补接口（原有功能）

- `GET /rubbings` / `POST /rubbings`
- `GET|POST /rubbings/:id/damages`
- `GET /damages?status=&type=`、`PATCH /damages/:id`
- `GET /batches`、`POST /batches`、`GET /batches/:id`、`POST /batches/:id/complete`

字段语义见服务启动后的 `GET /health`（返回完整路由表）。

## 数据文件

`data/db.json` 包含集合：`rubbings`、`damages`、`batches`、`images`、`jobs`、`results`。
服务首次启动时写入示例拓片数据；`*.tmp` 为崩溃残留的临时文件，启动时自动清理（它们永远不会被读取）。
