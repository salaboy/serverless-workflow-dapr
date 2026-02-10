package io.dapr.serverlessworkflow.activities;

import io.dapr.client.DaprClient;
import io.dapr.workflows.WorkflowActivity;
import io.dapr.workflows.WorkflowActivityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Dapr WorkflowActivity for emitting events via Dapr pub/sub.
 */
@Component
public class EmitEventActivity implements WorkflowActivity {

    private static final Logger logger = LoggerFactory.getLogger(EmitEventActivity.class);
    private final DaprClient daprClient;
    private final String pubsubName;

    public EmitEventActivity(
            DaprClient daprClient,
            @Value("${dapr.pubsub.name:pubsub}") String pubsubName) {
        this.daprClient = daprClient;
        this.pubsubName = pubsubName;
    }

    @Override
    public Object run(WorkflowActivityContext ctx) {
        EmitEventInput input = ctx.getInput(EmitEventInput.class);
        logger.info("Emitting event type: {} from source: {}", input.type(), input.source());

        try {
            // Create CloudEvent-like structure
            Map<String, Object> cloudEvent = Map.of(
                "specversion", "1.0",
                "type", input.type(),
                "source", input.source(),
                "data", input.data() != null ? input.data() : Map.of()
            );

            // Publish to Dapr pub/sub using the event type as the topic
            String topic = input.type();
            daprClient.publishEvent(pubsubName, topic, cloudEvent).block();

            logger.info("Event emitted successfully to topic: {}", topic);
            return new EmitEventOutput(true, topic);
        } catch (Exception e) {
            logger.error("Failed to emit event: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to emit event: " + e.getMessage(), e);
        }
    }

    /**
     * Input record for emit event activity.
     */
    public record EmitEventInput(
        String type,
        String source,
        Map<String, Object> data
    ) {}

    /**
     * Output record for emit event activity.
     */
    public record EmitEventOutput(
        boolean success,
        String topic
    ) {}
}
