package com.waterbox.ingest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.waterbox.ingest.api.EventAcceptedResponse;
import com.waterbox.ingest.api.EventIngestRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
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
        String key = partitionKey(request);
        String message = serialize(request);

        return kafkaTemplate.send(topic, key, message)
                .thenApply(result -> new EventAcceptedResponse(request.logId(), acceptedAt));
    }

    private String serialize(EventIngestRequest request) {
        try {
            return objectMapper.writeValueAsString(request);
        } catch (JsonProcessingException exception) {
            throw new EventSerializationException("Unable to serialize event", exception);
        }
    }

    private static String partitionKey(EventIngestRequest request) {
        Object projectId = request.properties().get("project_id");
        if (projectId instanceof String value && !value.isBlank()) {
            return value;
        }
        if (request.userId() != null && !request.userId().isBlank()) {
            return request.userId();
        }
        return request.logId();
    }
}
