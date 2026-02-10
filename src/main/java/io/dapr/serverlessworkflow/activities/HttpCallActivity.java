package io.dapr.serverlessworkflow.activities;

import io.dapr.workflows.WorkflowActivity;
import io.dapr.workflows.WorkflowActivityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Dapr WorkflowActivity for making HTTP calls.
 * Supports GET, POST, PUT, DELETE methods.
 */
@Component
public class HttpCallActivity implements WorkflowActivity {

    private static final Logger logger = LoggerFactory.getLogger(HttpCallActivity.class);
    private final HttpClient httpClient;

    public HttpCallActivity() {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    }

    @Override
    public Object run(WorkflowActivityContext ctx) {
        HttpCallInput input = ctx.getInput(HttpCallInput.class);
        logger.info("Executing HTTP {} to {}", input.method(), input.endpoint());

        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(input.endpoint()))
                .timeout(Duration.ofSeconds(60));

            // Add headers if provided
            if (input.headers() != null) {
                input.headers().forEach(requestBuilder::header);
            }

            // Set method and body
            switch (input.method().toUpperCase()) {
                case "GET" -> requestBuilder.GET();
                case "POST" -> requestBuilder.POST(
                    input.body() != null
                        ? HttpRequest.BodyPublishers.ofString(input.body())
                        : HttpRequest.BodyPublishers.noBody());
                case "PUT" -> requestBuilder.PUT(
                    input.body() != null
                        ? HttpRequest.BodyPublishers.ofString(input.body())
                        : HttpRequest.BodyPublishers.noBody());
                case "DELETE" -> requestBuilder.DELETE();
                default -> throw new IllegalArgumentException("Unsupported HTTP method: " + input.method());
            }

            HttpRequest request = requestBuilder.build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            logger.info("HTTP response status: {}", response.statusCode());

            return new HttpCallOutput(
                response.statusCode(),
                response.body(),
                response.headers().map()
            );
        } catch (Exception e) {
            logger.error("HTTP call failed: {}", e.getMessage(), e);
            throw new RuntimeException("HTTP call failed: " + e.getMessage(), e);
        }
    }

    /**
     * Input record for HTTP call activity.
     */
    public record HttpCallInput(
        String method,
        String endpoint,
        Map<String, String> headers,
        String body
    ) {
        public HttpCallInput(String method, String endpoint) {
            this(method, endpoint, null, null);
        }
    }

    /**
     * Output record for HTTP call activity.
     */
    public record HttpCallOutput(
        int statusCode,
        String content,
        Map<String, java.util.List<String>> headers
    ) {}
}
