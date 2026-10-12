package com.waterbox.ingest.api;

import java.time.Instant;

public record EventAcceptedResponse(String eventId, Instant acceptedAt) {
}
