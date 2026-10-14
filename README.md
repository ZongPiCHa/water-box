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

## 模拟埋点

启动升级后的服务后，使用 Python 3 脚本模拟 100 个用户，默认以每秒 2,000 条的目标速率持续发送，最多 200 个并发请求。使用 `Ctrl+C` 停止发送并等待在途请求完成：

```bash
python3 -m pip install --user --break-system-packages -r tools/requirements.txt
python3 tools/simulate_traffic.py
```

可修改发送间隔，或通过 `--count` 指定发送次数：

```bash
python3 tools/simulate_traffic.py --interval 1
python3 tools/simulate_traffic.py --count 10000 --qps 2000 --seed 42
python3 tools/simulate_traffic.py --endpoint http://服务器IP:8080/api/v1/events
python3 tools/simulate_traffic.py --dry-run --count 100 --interval 0
```

模拟数据生成只使用 Python 标准库；高吞吐 HTTP 发送使用 `aiohttp` 长连接。
`--users` 默认 100，`--env` 默认 `test`，可设为 `prod`；`--count` 为请求尝试次数，0 表示持续运行。
`--qps` 默认 2,000，以请求发起时间限速，不等待上一条响应；`--interval` 与 `--qps` 互斥，兼容低速或不限速发送。
`--max-active-projects` 默认 10,000，只限制参与后续随机互动的作品池，防止长期运行时内存和抽样开销随作品总量无限增长。
每秒输出发送速率、成功数、失败数和在途数。请求耗时过长达到 `--concurrency` 上限时会降速，实际成功写入速率取决于服务和 Kafka 性能。
2,000 QPS 平均每毫秒产生 2 条事件，因此不同事件可能具有相同 `timestamp`；毫秒时间戳用于表示发生时间，事件唯一性使用 `log_id`，链路关系使用 `span_id`。
`--dry-run` 仅将 JSON 输出到标准输出，不发送请求；启动信息和汇总输出到标准错误。随机种子固定行为序列，ID 和真实时间戳仍随运行变化。

| 主题/动作 | 上报端 | 模拟行为 |
| --- | --- | --- |
| `community/view` | `front` | 浏览作品，包含停留时长、来源页面 |
| `community/like`、`community/unlike` | `backend` | 点赞；约 10% 的点赞延迟取消 |
| `community/comment`、`community/delete_comment` | `backend` | 评论；约 10% 的评论由原作者延迟删除 |
| `community/share` | `front` | 前端生成转发链接，不生成后端转发事件 |
| `community/follow` | `backend` | 关注作品作者，同一用户不重复关注同一作者 |
| `community/publish` | `backend` | 发布个人或团队作品，团队作品带顶层 `team_id` |
| `login_page/view`、`login_page/login` | `front`、`backend` | 少量登录页浏览及登录成功事件 |
| `profile/update_username`、`team/join` | `backend` | 少量改名、加入团队，`user_id` 保持不变 |

模拟器初始化约 50 个历史作品，不发送它们的发布事件；新作品会先产生发布事件再参与互动。
作品使用 `project_id`，评论使用 `comment_id`，关注目标使用 `target_user_id`，这些业务字段都放在 `properties`。
作品热度和用户活跃度有差异，同一用户不能重复点赞同一作品，也不能关注自己。
互动先生成关联浏览，通过 `pre_span_id` 连接上游 `span_id`；用户登录或闲置 30 分钟后更换会话。
取消和删除会延迟 5～30 个生成步骤，因此短样本或运行末尾尚未执行的操作会让实际比例偏离 10%。不生成删除作品事件。

脚本仅模拟埋点，不调用真实社区业务 API。内存状态在脚本退出后重置，各次运行 ID 不重叠。
同一台机器只应运行一个模拟器实例，可用 `pgrep -af simulate_traffic.py` 检查。
脚本在服务返回 `202` 时视为 Kafka 已确认写入；失败消息沿用原 `log_id` 优先重试，重试也计入限速和 `--count`。并发发送时，关联事件的接收顺序可能与生成顺序不同；消费端应使用时间戳与链路字段关联。
服务端为每个收到的合法事件输出一条 `INFO` 日志，Kafka 异步发送失败输出 `ERROR`，不会记录完整 `properties`。
埋点日志只写入 `logs/events.log`，避免高频控制台输出阻塞 HTTP 请求线程；每天或每 100MB 滚动，保留 7 天，总量上限 1GB。
可通过 `--water-box.logging.path=/指定目录` 修改日志目录。

```bash
tail -f logs/events.log
```

## 吞吐配置

Kafka 生产端已启用幂等写入、`acks=all`、LZ4 压缩、64 KiB 批次、5 ms 批量等待和 128 MiB 缓冲区。Tomcat 配置为 400 个请求线程、10,000 个请求队列和 20,000 个连接。

这些配置面向 10,000 QPS 的目标。正式上线前仍需使用与生产一致的 TLS、网络、Kafka Broker 规格和请求体大小进行压测验证。
