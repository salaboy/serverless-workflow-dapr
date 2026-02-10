package io.dapr.serverlessworkflow.workflow;

import io.dapr.serverlessworkflow.parser.ServerlessWorkflowParser;
import io.dapr.workflows.client.DaprWorkflowClient;
import io.dapr.workflows.runtime.WorkflowRuntime;
import io.dapr.workflows.runtime.WorkflowRuntimeBuilder;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for Serverless Workflow definitions.
 * Parses workflow definitions and creates Dapr Workflow executors.
 */
@Component
public class ServerlessWorkflowRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ServerlessWorkflowRegistry.class);

    private final ServerlessWorkflowParser parser;
    private final Map<String, Workflow> workflowDefinitions = new ConcurrentHashMap<>();
    private final Map<String, ServerlessWorkflowExecutor> workflowExecutors = new ConcurrentHashMap<>();

    public ServerlessWorkflowRegistry(ServerlessWorkflowParser parser) {
        this.parser = parser;
    }

    @PostConstruct
    public void init() throws IOException {
        loadWorkflowsFromClasspath();
    }

    /**
     * Load all YAML workflow definitions from the classpath.
     */
    public void loadWorkflowsFromClasspath() throws IOException {
        logger.info("Loading workflow definitions from classpath...");
        List<Workflow> workflows = parser.parseAllYamlWorkflows();

        for (Workflow workflow : workflows) {
            registerWorkflow(workflow);
        }

        logger.info("Loaded {} workflow definitions", workflowDefinitions.size());
    }

    /**
     * Register a workflow definition.
     *
     * @param workflow the workflow definition to register
     */
    public void registerWorkflow(Workflow workflow) {
        String workflowId = getWorkflowId(workflow);
        workflowDefinitions.put(workflowId, workflow);

        ServerlessWorkflowExecutor executor = new ServerlessWorkflowExecutor(workflow);
        workflowExecutors.put(workflowId, executor);

        logger.info("Registered workflow: {}", workflowId);
    }

    /**
     * Get a workflow definition by ID.
     *
     * @param workflowId the workflow ID (namespace.name)
     * @return the workflow definition, or null if not found
     */
    public Workflow getWorkflowDefinition(String workflowId) {
        return workflowDefinitions.get(workflowId);
    }

    /**
     * Get a workflow executor by ID.
     *
     * @param workflowId the workflow ID (namespace.name)
     * @return the workflow executor, or null if not found
     */
    public ServerlessWorkflowExecutor getWorkflowExecutor(String workflowId) {
        return workflowExecutors.get(workflowId);
    }

    /**
     * Get all registered workflow IDs.
     *
     * @return map of workflow IDs to their definitions
     */
    public Map<String, Workflow> getAllWorkflows() {
        return new HashMap<>(workflowDefinitions);
    }

    /**
     * Get all workflow executors.
     *
     * @return map of workflow IDs to their executors
     */
    public Map<String, ServerlessWorkflowExecutor> getAllExecutors() {
        return new HashMap<>(workflowExecutors);
    }

    /**
     * Check if a workflow is registered.
     *
     * @param workflowId the workflow ID
     * @return true if registered
     */
    public boolean hasWorkflow(String workflowId) {
        return workflowDefinitions.containsKey(workflowId);
    }

    /**
     * Build a Dapr WorkflowRuntime with all registered workflows.
     *
     * @param builder the WorkflowRuntimeBuilder to use
     * @return the configured WorkflowRuntime
     */
    public WorkflowRuntime buildRuntime(WorkflowRuntimeBuilder builder) {
        for (Map.Entry<String, ServerlessWorkflowExecutor> entry : workflowExecutors.entrySet()) {
            logger.info("Registering Dapr workflow: {}", entry.getKey());
            builder.registerWorkflow(entry.getValue().getClass());
        }
        return builder.build();
    }

    /**
     * Generate a workflow ID from the workflow definition.
     */
    private String getWorkflowId(Workflow workflow) {
        return workflow.getDocument().getNamespace() + "." + workflow.getDocument().getName();
    }
}
