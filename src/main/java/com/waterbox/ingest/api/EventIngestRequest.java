package com.waterbox.ingest.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public record EventIngestRequest(
        @NotBlank @Size(max = 128) String eventType,
        @NotNull Instant occurredAt,
        @Size(max = 128) String userId,
        @Size(max = 128) String communityId,
        @Size(max = 128) String designId,
        @NotNull JsonNode payload
) {
}
