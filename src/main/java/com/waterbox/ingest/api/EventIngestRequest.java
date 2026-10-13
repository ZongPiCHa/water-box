package com.waterbox.ingest.api;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.Map;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record EventIngestRequest(
        @NotNull @Positive Long timestamp,
        @NotBlank @Size(max = 128) String logId,
        @NotBlank @Size(max = 128) String sessionId,
        @NotBlank @Size(max = 128) String spanId,
        @Size(max = 128) String preSpanId,
        @NotBlank @Pattern(regexp = "front|backend") String system,
        @NotBlank @Size(max = 128) String theme,
        @NotBlank @Size(max = 128) String action,
        @Size(max = 128) String userId,
        @Size(max = 128) String teamId,
        @Size(max = 128) String username,
        @NotBlank @Pattern(regexp = "prod|test") String env,
        @NotNull Map<String, Object> properties
) {
}
