package com.waterbox.ingest.api;

import com.waterbox.ingest.service.EventSerializationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler({KafkaException.class, EventSerializationException.class})
    ProblemDetail handlePublishingFailure(RuntimeException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Event ingestion is temporarily unavailable"
        );
        problem.setTitle("Event publishing failed");
        return problem;
    }
}
