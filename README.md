# water-box

`water-box` accepts HTTPS telemetry events from a community application and durably publishes them to Kafka.

## Runtime contract

- Endpoint: `POST /api/v1/events`
- Default listener: `8443`
- Kafka broker: `10.37.215.3:9092`
- Kafka topic: `community.events.v1`
- Success: `202 Accepted`, only after Kafka acknowledges the write
- Temporary Kafka failure: `503 Service Unavailable`

Example request:

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

`eventType`, `occurredAt`, and `payload` are required. Kafka partitioning prioritizes `designId`, then `communityId`, then `userId`, preserving order for one entity.

## Local development

Run tests:

```bash
./mvnw test
```

Start without TLS for local development:

```bash
./mvnw spring-boot:run
```

Configure a PKCS#12 certificate before exposing the service:

```bash
export SERVER_SSL_ENABLED=true
export SERVER_SSL_KEY_STORE=/absolute/path/water-box.p12
export SERVER_SSL_KEY_STORE_PASSWORD=changeit
export SERVER_SSL_KEY_ALIAS=water-box
./mvnw spring-boot:run
```

## Throughput configuration

Kafka producer idempotence, `acks=all`, LZ4 compression, 64 KiB batching, 5 ms linger, and 128 MiB buffering are enabled. Tomcat is configured for 400 request threads, a 10,000 request backlog, and 20,000 connections. Capacity must be validated with production-equivalent TLS, network, broker, and payload sizes before committing to 10,000 QPS.
