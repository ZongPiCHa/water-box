# water-box

`water-box` 接收社区应用通过 HTTPS 上报的埋点事件，并可靠地写入 Kafka。

## 服务约定

- 接口：`POST /api/v1/events`
- 默认监听端口：`8443`
- Kafka Broker：`10.37.215.3:9092`
- Kafka Topic：`community.events.v1`
- 成功响应：Kafka 确认写入后返回 `202 Accepted`
- Kafka 临时不可用：返回 `503 Service Unavailable`

请求示例：

```bash
curl --request POST https://localhost:8443/api/v1/events \
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

本地开发时可不开启 TLS：

```bash
./mvnw spring-boot:run
```

对外提供服务前，请配置 PKCS#12 格式证书：

```bash
export SERVER_SSL_ENABLED=true
export SERVER_SSL_KEY_STORE=/absolute/path/water-box.p12
export SERVER_SSL_KEY_STORE_PASSWORD=changeit
export SERVER_SSL_KEY_ALIAS=water-box
./mvnw spring-boot:run
```

## 吞吐配置

Kafka 生产端已启用幂等写入、`acks=all`、LZ4 压缩、64 KiB 批次、5 ms 批量等待和 128 MiB 缓冲区。Tomcat 配置为 400 个请求线程、10,000 个请求队列和 20,000 个连接。

这些配置面向 10,000 QPS 的目标。正式上线前仍需使用与生产一致的 TLS、网络、Kafka Broker 规格和请求体大小进行压测验证。
