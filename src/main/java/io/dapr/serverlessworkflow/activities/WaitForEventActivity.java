package io.dapr.serverlessworkflow.activities;

import io.dapr.workflows.WorkflowActivity;
import io.dapr.workflows.WorkflowActivityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Dapr WorkflowActivity placeholder for waiting on external events.
 * In Dapr Workflows, external events are typically handled via workflow.waitForExternalEvent()
 * rather than an activity. This activity serves as a marker/placeholder for event listening.
 */
@Component
public class WaitForEventActivity implements WorkflowActivity {

    private static final Logger logger = LoggerFactory.getLogger(WaitForEventActivity.class);

    @Override
    public Object run(WorkflowActivityContext ctx) {
        WaitForEventInput input = ctx.getInput(WaitForEventInput.class);
        logger.info("WaitForEvent activity invoked for event type: {}", input.eventType());

        // This activity is a placeholder - actual event waiting should be done
        // via WorkflowContext.waitForExternalEvent() in the workflow itself.
        // This activity can be used to set up subscriptions or perform pre-event logic.

        return new WaitForEventOutput(
            input.eventType(),
            "Event listener configured for type: " + input.eventType()
        );
    }

    /**
     * Input record for wait for event activity.
     */
    public record WaitForEventInput(
        String eventType,
        String condition
    ) {}

    /**
     * Output record for wait for event activity.
     */
    public record WaitForEventOutput(
        String eventType,
        String message
    ) {}
}
