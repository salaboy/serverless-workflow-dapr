package io.dapr.serverlessworkflow.workflow;

import io.dapr.serverlessworkflow.activities.EmitEventActivity;
import io.dapr.serverlessworkflow.activities.HttpCallActivity;
import io.dapr.serverlessworkflow.activities.WaitForEventActivity;
import io.dapr.workflows.runtime.WorkflowRuntime;
import io.dapr.workflows.runtime.WorkflowRuntimeBuilder;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Factory for creating and configuring Dapr Workflow runtimes
 * from Serverless Workflow definitions.
 */
@Component
public class DynamicWorkflowFactory {

    private static final Logger logger = LoggerFactory.getLogger(DynamicWorkflowFactory.class);

    private final ServerlessWorkflowRegistry registry;

    public DynamicWorkflowFactory(ServerlessWorkflowRegistry registry) {
        this.registry = registry;
    }

    /**
     * Create a WorkflowRuntimeBuilder configured with all registered workflows and activities.
     *
     * @return configured WorkflowRuntimeBuilder
     */
    public WorkflowRuntimeBuilder createRuntimeBuilder() {
        WorkflowRuntimeBuilder builder = new WorkflowRuntimeBuilder();

        // Register all activities
        registerActivities(builder);

        // Register all workflows
        registerWorkflows(builder);

        return builder;
    }

    /**
     * Create and start a WorkflowRuntime with all registered workflows.
     *
     * @return the started WorkflowRuntime
     */
    public WorkflowRuntime createAndStartRuntime() {
        WorkflowRuntimeBuilder builder = createRuntimeBuilder();
        WorkflowRuntime runtime = builder.build();
        logger.info("Created Dapr WorkflowRuntime with {} workflows",
            registry.getAllWorkflows().size());
        return runtime;
    }

    private void registerActivities(WorkflowRuntimeBuilder builder) {
        logger.info("Registering workflow activities...");

        // Register HTTP call activity
        builder.registerActivity(HttpCallActivity.class);
        logger.debug("Registered activity: {}", HttpCallActivity.class.getName());

        // Register emit event activity
        builder.registerActivity(EmitEventActivity.class);
        logger.debug("Registered activity: {}", EmitEventActivity.class.getName());

        // Register wait for event activity
        builder.registerActivity(WaitForEventActivity.class);
        logger.debug("Registered activity: {}", WaitForEventActivity.class.getName());
    }

    private void registerWorkflows(WorkflowRuntimeBuilder builder) {
        logger.info("Registering workflows from registry...");

        Map<String, ServerlessWorkflowExecutor> executors = registry.getAllExecutors();
        for (Map.Entry<String, ServerlessWorkflowExecutor> entry : executors.entrySet()) {
            String workflowId = entry.getKey();
            ServerlessWorkflowExecutor executor = entry.getValue();

            // Create a named workflow class for each definition
            builder.registerWorkflow(executor.getClass());
            logger.info("Registered workflow: {}", workflowId);
        }
    }

    /**
     * Get workflow metadata for a specific workflow.
     *
     * @param workflowId the workflow ID
     * @return workflow metadata or null
     */
    public WorkflowMetadata getWorkflowMetadata(String workflowId) {
        Workflow workflow = registry.getWorkflowDefinition(workflowId);
        if (workflow == null) {
            return null;
        }

        return new WorkflowMetadata(
            workflowId,
            workflow.getDocument().getName(),
            workflow.getDocument().getNamespace(),
            workflow.getDocument().getVersion()
        );
    }

    /**
     * Metadata record for a workflow.
     */
    public record WorkflowMetadata(
        String id,
        String name,
        String namespace,
        String version
    ) {}
}
