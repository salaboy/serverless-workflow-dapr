package io.dapr.serverlessworkflow.activities;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for HttpCallActivity input/output records.
 */
class HttpCallActivityTest {

    @Test
    void shouldCreateHttpCallInput() {
        HttpCallActivity.HttpCallInput input = new HttpCallActivity.HttpCallInput(
            "GET",
            "https://api.example.com/data"
        );

        assertEquals("GET", input.method());
        assertEquals("https://api.example.com/data", input.endpoint());
        assertNull(input.headers());
        assertNull(input.body());
    }

    @Test
    void shouldCreateHttpCallInputWithAllParams() {
        Map<String, String> headers = Map.of("Authorization", "Bearer token");
        String body = "{\"key\": \"value\"}";

        HttpCallActivity.HttpCallInput input = new HttpCallActivity.HttpCallInput(
            "POST",
            "https://api.example.com/data",
            headers,
            body
        );

        assertEquals("POST", input.method());
        assertEquals("https://api.example.com/data", input.endpoint());
        assertEquals(headers, input.headers());
        assertEquals(body, input.body());
    }

    @Test
    void shouldCreateHttpCallOutput() {
        Map<String, java.util.List<String>> headers = Map.of(
            "Content-Type", java.util.List.of("application/json")
        );

        HttpCallActivity.HttpCallOutput output = new HttpCallActivity.HttpCallOutput(
            200,
            "{\"result\": \"success\"}",
            headers
        );

        assertEquals(200, output.statusCode());
        assertEquals("{\"result\": \"success\"}", output.content());
        assertEquals(headers, output.headers());
    }
}
