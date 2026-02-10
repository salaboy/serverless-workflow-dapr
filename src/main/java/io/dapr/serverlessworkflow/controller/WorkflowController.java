package io.dapr.serverlessworkflow.controller;

import io.dapr.serverlessworkflow.workflow.DynamicWorkflowFactory;
import io.dapr.serverlessworkflow.workflow.ServerlessWorkflowExecutor;
import io.dapr.serverlessworkflow.workflow.ServerlessWorkflowRegistry;
import io.dapr.workflows.client.DaprWorkflowClient;
import io.dapr.workflows.client.WorkflowInstanceStatus;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * REST controller for managing and executing Serverless Workflows via Dapr.
 */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowController.class);

    private final ServerlessWorkflowRegistry registry;
    private final DynamicWorkflowFactory factory;
    private final DaprWorkflowClient workflowClient;

    public WorkflowController(ServerlessWorkflowRegistry registry,
                             DynamicWorkflowFactory factory,
                             DaprWorkflowClient workflowClient) {
        this.registry = registry;
        this.factory = factory;
        this.workflowClient = workflowClient;
    }

    /**
     * List all registered workflows.
     */
    @GetMapping
    public ResponseEntity<List<WorkflowInfo>> listWorkflows() {
        List<WorkflowInfo> workflows = registry.getAllWorkflows().entrySet().stream()
            .map(entry -> {
                Workflow wf = entry.getValue();
                return new WorkflowInfo(
                    entry.getKey(),
                    wf.getDocument().getName(),
                    wf.getDocument().getNamespace(),
                    wf.getDocument().getVersion()
                );
            })
            .collect(Collectors.toList());

        return ResponseEntity.ok(workflows);
    }

    /**
     * Get details of a specific workflow.
     */
    @GetMapping("/{workflowId}")
    public ResponseEntity<WorkflowInfo> getWorkflow(@PathVariable String workflowId) {
        Workflow workflow = registry.getWorkflowDefinition(workflowId);
        if (workflow == null) {
            return ResponseEntity.notFound().build();
        }

        WorkflowInfo info = new WorkflowInfo(
            workflowId,
            workflow.getDocument().getName(),
            workflow.getDocument().getNamespace(),
            workflow.getDocument().getVersion()
        );

        return ResponseEntity.ok(info);
    }

    /**
     * Start a workflow instance.
     */
    @PostMapping("/{workflowId}/start")
    public ResponseEntity<WorkflowInstanceInfo> startWorkflow(
            @PathVariable String workflowId,
            @RequestBody(required = false) Map<String, Object> input) {

        ServerlessWorkflowExecutor executor = registry.getWorkflowExecutor(workflowId);
        if (executor == null) {
            return ResponseEntity.notFound().build();
        }

        String instanceId = UUID.randomUUID().toString();

        try {
            logger.info("Starting workflow {} with instance ID {}", workflowId, instanceId);

            // Schedule the workflow with input and instance ID
            workflowClient.scheduleNewWorkflow(
                executor.getClass(),
                input,
                instanceId
            );

            WorkflowInstanceInfo instanceInfo = new WorkflowInstanceInfo(
                instanceId,
                workflowId,
                "SCHEDULED",
                null
            );

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(instanceInfo);
        } catch (Exception e) {
            logger.error("Failed to start workflow {}: {}", workflowId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Get the status of a workflow instance.
     */
    @GetMapping("/instances/{instanceId}")
    public ResponseEntity<WorkflowInstanceInfo> getWorkflowStatus(@PathVariable String instanceId) {
        try {
            WorkflowInstanceStatus status = workflowClient.getInstanceState(instanceId, true);
            if (status == null) {
                return ResponseEntity.notFound().build();
            }

            WorkflowInstanceInfo info = new WorkflowInstanceInfo(
                instanceId,
                status.getName(),
                status.getRuntimeStatus().name(),
                status.readOutputAs(Object.class)
            );

            return ResponseEntity.ok(info);
        } catch (Exception e) {
            logger.error("Failed to get workflow status for {}: {}", instanceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Wait for a workflow instance to complete.
     */
    @GetMapping("/instances/{instanceId}/wait")
    public ResponseEntity<WorkflowInstanceInfo> waitForWorkflow(
            @PathVariable String instanceId,
            @RequestParam(defaultValue = "60") int timeoutSeconds) {
        try {
            WorkflowInstanceStatus status = workflowClient.waitForInstanceCompletion(
                instanceId,
                Duration.ofSeconds(timeoutSeconds),
                true
            );

            if (status == null) {
                return ResponseEntity.notFound().build();
            }

            WorkflowInstanceInfo info = new WorkflowInstanceInfo(
                instanceId,
                status.getName(),
                status.getRuntimeStatus().name(),
                status.readOutputAs(Object.class)
            );

            return ResponseEntity.ok(info);
        } catch (Exception e) {
            logger.error("Failed to wait for workflow {}: {}", instanceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Terminate a workflow instance.
     */
    @PostMapping("/instances/{instanceId}/terminate")
    public ResponseEntity<Void> terminateWorkflow(@PathVariable String instanceId) {
        try {
            workflowClient.terminateWorkflow(instanceId, null);
            return ResponseEntity.accepted().build();
        } catch (Exception e) {
            logger.error("Failed to terminate workflow {}: {}", instanceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Raise an external event to a workflow instance.
     */
    @PostMapping("/instances/{instanceId}/events/{eventName}")
    public ResponseEntity<Void> raiseEvent(
            @PathVariable String instanceId,
            @PathVariable String eventName,
            @RequestBody(required = false) Object eventData) {
        try {
            logger.info("Raising event {} for workflow instance {}", eventName, instanceId);
            workflowClient.raiseEvent(instanceId, eventName, eventData);
            return ResponseEntity.accepted().build();
        } catch (Exception e) {
            logger.error("Failed to raise event for workflow {}: {}", instanceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Workflow information record.
     */
    public record WorkflowInfo(
        String id,
        String name,
        String namespace,
        String version
    ) {}

    /**
     * Workflow instance information record.
     */
    public record WorkflowInstanceInfo(
        String instanceId,
        String workflowId,
        String status,
        Object output
    ) {}
}
