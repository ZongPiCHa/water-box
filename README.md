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
    "eventType": "design.viewed",
    "occurredAt": "2026-10-12T10:15:30Z",
    "userId": "user-123",
    "communityId": "community-42",
    "designId": "design-9001",
    "payload": {"referrer": "home"}
  }'
```

`eventType`、`occurredAt` 和 `payload` 为必填字段。Kafka 分区键依次优先选择 `designId`、`communityId`、`userId`，以保证同一实体的事件顺序。

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
export SERVER_SSL_KEY_STORE=/absolute/path/water-box.p12
export SERVER_SSL_KEY_STORE_PASSWORD=changeit
export SERVER_SSL_KEY_ALIAS=water-box
./mvnw spring-boot:run
```

## 模拟埋点

启动服务后，使用标准库脚本持续发送测试事件，默认每 5 秒一条。使用 `Ctrl+C` 停止：

```bash
python3 tools/simulate_traffic.py
```

可修改发送间隔，或通过 `--count` 指定发送次数：

```bash
python3 tools/simulate_traffic.py --interval 1
python3 tools/simulate_traffic.py --count 10
```

脚本在服务返回 `202` 时视为 Kafka 已确认写入；若返回非 `202` 或无法连接服务，会输出失败详情。服务端会为每个收到的事件输出一条 `INFO` 日志，但不会记录完整 payload。

## 吞吐配置

Kafka 生产端已启用幂等写入、`acks=all`、LZ4 压缩、64 KiB 批次、5 ms 批量等待和 128 MiB 缓冲区。Tomcat 配置为 400 个请求线程、10,000 个请求队列和 20,000 个连接。

这些配置面向 10,000 QPS 的目标。正式上线前仍需使用与生产一致的 TLS、网络、Kafka Broker 规格和请求体大小进行压测验证。
