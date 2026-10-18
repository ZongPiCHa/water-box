# water-box

`water-box` 接收社区应用上报的埋点事件，并可靠地写入 Kafka。当前默认使用 HTTP，后续可通过配置开启 HTTPS。

## 服务约定

- 接口：`POST /api/v1/events`
- 默认监听端口：`8080`
- Kafka Broker：`10.37.215.3:9092`
- Kafka Topic：`community.events.v1`
- 成功响应：Kafka 确认写入后返回 `202 Accepted`
- Kafka 临时不可用：返回 `503 Service Unavailable`

请求示例：

```bash
curl --request POST http://localhost:8080/api/v1/events \
  --header 'Content-Type: application/json' \
  --data '{
    "timestamp": 1791849600000,
    "log_id": "log-001",
    "session_id": "session-001",
    "span_id": "span-002",
    "pre_span_id": "span-001",
    "system": "backend",
    "theme": "community",
    "action": "like",
    "user_id": "user-123",
    "team_id": null,
    "username": "designer",
    "env": "test",
    "properties": {
      "project_id": "project-9001",
      "author_user_id": "user-456",
      "location": "america",
      "duration": 1200
    }
  }'
```

## 埋点协议

| 字段 | 约定 |
| --- | --- |
| `timestamp` | 必填，13 位正整数 Unix 毫秒时间戳，使用 `time.time_ns() // 1_000_000` 生成 |
| `log_id` | 必填，发送方生成的一次埋点唯一 ID，重试沿用同一个值 |
| `session_id` | 必填，用户会话 ID，同一会话中的多次动作共用 |
| `span_id` | 必填，本次操作链路 ID |
| `pre_span_id` | 上游链路 ID，根链路为空 |
| `system` | 必填，`front`（前端）或 `backend`（后端） |
| `theme` | 必填，业务主题，例如 `login_page`、`community` |
| `action` | 必填，最小动作，例如 `login`、`register`、`like` |
| `user_id` | 用户稳定 ID，匿名访问可为空 |
| `team_id` | 操作所属团队 ID，个人操作可为空；以团队发布作品时填写 |
| `username` | 动作发生时的用户名，可变化，不能作为用户唯一标识 |
| `env` | 必填，`prod` 或 `test` |
| `properties` | 必填 JSON 对象，可为 `{}`，支持字符串、数值、布尔、数组、嵌套对象 |

通用字符串最长 128 字符。Kafka 消息使用以上 snake_case 字段，保留发送方的 `log_id`、时间戳与自定义属性，不额外生成事件 ID。
分区键优先使用 `properties.project_id`，其次为 `user_id`，匿名事件使用 `log_id`。同一分区内按 Kafka 实际接收顺序排列，多发送方并发上报时不保证业务时间顺序。

成功响应示例：

```json
{"log_id":"log-001","accepted_at":"2026-10-13T00:00:00Z"}
```

这是对旧版 `eventType/occurredAt/payload` 协议的替换，发送方需要同步升级。
接收服务不验证作品、评论等业务是否真实存在，也不按 `log_id` 去重；客户端超时不代表消息一定未写入，重试可能产生相同 `log_id` 的多条记录，消费端应据此去重。

## 本地开发

运行测试：

```bash
./mvnw test
```

临时 HTTP 启动：

```bash
./mvnw spring-boot:run
```

临时 HTTP 仅应在可信内网使用。后续需要 HTTPS 时，配置 PKCS#12 格式证书：

```bash
export SERVER_SSL_ENABLED=true
export SERVER_PORT=8443
export SERVER_SSL_KEY_STORE=/absolute/path/water-box.p12
export SERVER_SSL_KEY_STORE_PASSWORD=changeit
export SERVER_SSL_KEY_ALIAS=water-box
./mvnw spring-boot:run
```

## 真实社区行为模拟

脚本调用线上 water_community 业务接口，互动会修改真实数据库，由社区后端生成埋点。默认每秒模拟 100 个行为，其中 50% 是不发请求的浏览，因此业务请求约 50 次/秒；最多 10 个在途请求。`Ctrl+C` 停止发起新行为并等待在途请求完成。

```bash
python3 -m pip install --user --break-system-packages -r tools/requirements.txt
python3 tools/simulate_traffic.py
```

模拟器启动时从 water_community 的 `GET /api/simulation/catalog` 读取真实的用户、作品、已有点赞和关注关系，因此需要先启动 water_community（默认 `http://127.0.0.1:8090`）：

```bash
python3 tools/simulate_traffic.py --interval 1
python3 tools/simulate_traffic.py --count 10000 --qps 100 --seed 42
python3 tools/simulate_traffic.py --community-url http://服务器IP:8090
python3 tools/simulate_traffic.py --dry-run --count 100 --interval 0
```

`--count` 为行为总数，包含浏览及取消操作，0 表示持续运行。偶数个行为中浏览严格占 50%，奇数时多一次浏览。浏览只生成本地上下文和统计，既不调用 `/views`，也不向 water-box 发送 `view_page`。
`--qps` 是包含浏览的行为速率，默认 100；`--interval` 与其互斥。达到并发上限时会降速，恢复后不会突发补发积压请求。
每秒输出行为数、浏览数、HTTP 请求数、确认成功数、未确认成功数和在途数；结束时输出各行为及待执行取消数量。
`--dry-run` 仍读取一次真实目录，但只打印后续请求计划，不调用写接口，也不会虚构成功结果或评论 ID，因此不安排取消操作。
旧 `--endpoint`、`--env` 已移除：埋点入口和环境由 water_community 的 `WATER_BOX_ENDPOINT`、`WATER_BOX_ENV` 配置。脚本不会重复直发后端埋点。

| 行为 | 实际请求 | 成功判断 |
| --- | --- | --- |
| `view_page` | 无，仅本地浏览 | 不计入 HTTP 成功数 |
| `like` / `unlike` | `POST /api/projects/{id}/likes` | HTTP 200，`active` 分别为 true / false |
| `comment` | `POST /api/projects/{id}/comments`，JSON `{"content":"…"}` | HTTP 201，返回非空评论 ID |
| `delete_comment` | `DELETE /api/projects/{id}/comments/{comment_id}` | HTTP 204 |
| `share` | `POST /api/projects/{id}/shares` | HTTP 201，`active=true` |
| `follow` | `POST /api/users/{authorId}/follow?projectId={id}` | HTTP 200，`active=true` |

请求携带 `X-User-Id`、`X-Session-Id` 和可用的 `X-Span-Id`；用户闲置 30 分钟后轮换会话。浏览链路只保存在本地，因此可能出现没有对应已上报浏览事件的 `pre_span_id`。
用户、作品、作者、已有点赞和关注来自真实目录。后端成功响应后才更新本地关系，新增评论 ID 必须采用后端返回值。只有本轮确认成功的点赞和评论会按 10% 概率安排取消，延迟 5～30 个行为步骤，并沿用原操作用户。
已有点赞、关注以及同一关系的在途请求会被跳过；不会主动点赞自己的作品或关注自己。统计成功代表业务成功，不代表 Kafka 投递已确认，后者查看社区后端与 water-box 日志。

现有点赞、关注接口是状态切换接口；超时可能已经写入，自动重试可能反向取消。因此写请求失败或响应状态不符时不重试，同一点赞/关注关系在本轮不再操作。评论、转发和删除失败也不重试，以免重复创建。
同一社区只运行一个模拟器；多个实例或其他客户端并发修改相同关系仍可能导致目录状态过时，遇到状态不符会记录为未确认成功。可用 `pgrep -af simulate_traffic.py` 检查。
停止时尚未到期的取消不会强行补发，短样本实际取消比例可能偏离 10%；停止再启动会重新读取目录，不恢复上轮待执行取消。

服务端为每个收到的合法事件输出一条 `INFO` 日志，Kafka 异步发送失败输出 `ERROR`，不会记录完整 `properties`。
埋点日志只写入 `logs/events.log`，避免高频控制台输出阻塞 HTTP 请求线程；每天或每 100MB 滚动，保留 7 天，总量上限 1GB。
可通过 `--water-box.logging.path=/指定目录` 修改日志目录。

## 吞吐配置

Kafka 生产端已启用幂等写入、`acks=all`、LZ4 压缩、64 KiB 批次、5 ms 批量等待和 128 MiB 缓冲区。Tomcat 配置为 400 个请求线程、10,000 个请求队列和 20,000 个连接。

这些配置面向 10,000 QPS 的目标。正式上线前仍需使用与生产一致的 TLS、网络、Kafka Broker 规格和请求体大小进行压测验证。
