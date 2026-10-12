package com.waterbox.ingest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.waterbox.ingest.api.EventAcceptedResponse;
import com.waterbox.ingest.api.EventIngestRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final Clock clock;

    @Autowired
    public EventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${water-box.kafka.topic}") String topic
    ) {
        this(kafkaTemplate, objectMapper, topic, Clock.systemUTC());
    }

    EventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            String topic,
            Clock clock
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.clock = clock;
    }

    public CompletableFuture<EventAcceptedResponse> publish(EventIngestRequest request) {
        Instant acceptedAt = clock.instant();
        String eventId = UUID.randomUUID().toString();
        String key = partitionKey(request, eventId);
        String message = serialize(request, eventId, acceptedAt);

        return kafkaTemplate.send(topic, key, message)
                .thenApply(result -> new EventAcceptedResponse(eventId, acceptedAt));
    }

    private String serialize(EventIngestRequest request, String eventId, Instant acceptedAt) {
        ObjectNode event = objectMapper.createObjectNode();
        event.put("eventId", eventId);
        event.put("eventType", request.eventType());
        event.put("occurredAt", request.occurredAt().toString());
        event.put("acceptedAt", acceptedAt.toString());
        putIfPresent(event, "userId", request.userId());
        putIfPresent(event, "communityId", request.communityId());
        putIfPresent(event, "designId", request.designId());
        event.set("payload", request.payload());
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new EventSerializationException("Unable to serialize event", exception);
        }
    }

    private static void putIfPresent(ObjectNode event, String fieldName, String value) {
        if (value != null) {
            event.put(fieldName, value);
        }
    }

    private static String partitionKey(EventIngestRequest request, String eventId) {
        if (request.designId() != null) {
            return request.designId();
        }
        if (request.communityId() != null) {
            return request.communityId();
        }
        if (request.userId() != null) {
            return request.userId();
        }
        return eventId;
    }
}
