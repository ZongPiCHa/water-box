package com.waterbox.ingest.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=localhost:9092")
@AutoConfigureMockMvc
class EventControllerTest {
    private static final String EVENT = """
            {
              "timestamp": 1791800000000,
              "log_id": "log-1",
              "session_id": "session-1",
              "span_id": "span-1",
              "pre_span_id": null,
              "system": "backend",
              "theme": "community",
              "action": "publish",
              "user_id": "user-1",
              "team_id": "team-1",
              "username": "designer",
              "env": "test",
              "properties": {"project_id": "project-42", "location": "america",
                             "duration": 1200, "nested": {"tags": ["ui", "app"]}}
            }
            """;

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;
    @MockitoBean KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void waitsForKafkaAndPreservesEventSchema() throws Exception {
        var acknowledgement = new CompletableFuture<SendResult<String, String>>();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(acknowledgement);
        var result = mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON).content(EVENT))
                .andExpect(request().asyncStarted()).andReturn();
        assertFalse(acknowledgement.isDone());
        var message = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("community.events.v1"), eq("project-42"), message.capture());
        assertEquals(objectMapper.readTree(EVENT), objectMapper.readTree(message.getValue()));
        acknowledgement.complete(null);
        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.log_id").value("log-1"))
                .andExpect(jsonPath("$.accepted_at").isNotEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\"system\":\"invalid\"", "\"env\":\"dev\"", "\"timestamp\":-1",
            "\"log_id\":\"\"", "\"session_id\":\"\"", "\"span_id\":\"\"",
            "\"theme\":\"\"", "\"action\":\"\"", "\"properties\":[]", "\"properties\":null"
    })
    void rejectsInvalidCommonFields(String replacement) throws Exception {
        var json = objectMapper.readTree(EVENT);
        var fragment = objectMapper.readTree("{" + replacement + "}");
        fragment.fields().forEachRemaining(entry ->
                ((com.fasterxml.jackson.databind.node.ObjectNode) json).set(entry.getKey(), entry.getValue()));
        mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(json)))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void returnsUnavailableOnKafkaFailure() throws Exception {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker unavailable")));
        var result = mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
                        .content(EVENT)).andReturn();
        mockMvc.perform(asyncDispatch(result)).andExpect(status().isServiceUnavailable());
    }

    @Test
    void allowsAnonymousRootEventAndEmptyProperties() throws Exception {
        var json = (com.fasterxml.jackson.databind.node.ObjectNode) objectMapper.readTree(EVENT);
        json.putNull("user_id");
        json.putNull("team_id");
        json.putNull("username");
        json.set("properties", objectMapper.createObjectNode());
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        var result = mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(json))).andReturn();
        mockMvc.perform(asyncDispatch(result)).andExpect(status().isAccepted());
        verify(kafkaTemplate).send(eq("community.events.v1"), eq("log-1"), anyString());
    }
}
