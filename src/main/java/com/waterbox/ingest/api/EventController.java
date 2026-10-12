package com.waterbox.ingest.api;

import com.waterbox.ingest.service.EventPublisher;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventPublisher eventPublisher;

    public EventController(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<EventAcceptedResponse>> ingest(
            @Valid @RequestBody EventIngestRequest request
    ) {
        return eventPublisher.publish(request)
                .handle((response, exception) -> {
                    if (exception != null) {
                        return ResponseEntity.<EventAcceptedResponse>status(HttpStatus.SERVICE_UNAVAILABLE).build();
                    }
                    return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
                });
    }
}
