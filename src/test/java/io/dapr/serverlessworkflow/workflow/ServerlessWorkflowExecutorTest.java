package io.dapr.serverlessworkflow.workflow;

import io.dapr.serverlessworkflow.parser.ServerlessWorkflowParser;
import io.dapr.workflows.Workflow;
import io.dapr.workflows.WorkflowStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the ServerlessWorkflowExecutor.
 */
class ServerlessWorkflowExecutorTest {

    private ServerlessWorkflowParser parser;

    @BeforeEach
    void setUp() {
        parser = new ServerlessWorkflowParser();
    }

    @Test
    void shouldCreateWorkflowFromStarWarsDefinition() throws IOException {
        io.serverlessworkflow.api.types.Workflow workflowDef = parser.parseFromClasspath("startwars.yaml");

        ServerlessWorkflowExecutor executor = new ServerlessWorkflowExecutor(workflowDef);
        assertNotNull(executor);

        // Verify it implements Workflow
        assertTrue(executor instanceof Workflow);

        // Verify it creates a WorkflowStub
        WorkflowStub stub = executor.create();
        assertNotNull(stub);
    }

    @Test
    void shouldCreateWorkflowFromGetDefinition() throws IOException {
        io.serverlessworkflow.api.types.Workflow workflowDef = parser.parseFromClasspath("get.yaml");

        ServerlessWorkflowExecutor executor = new ServerlessWorkflowExecutor(workflowDef);
        assertNotNull(executor);

        WorkflowStub stub = executor.create();
        assertNotNull(stub);
    }

    @Test
    void shouldCreateWorkflowFromEmitDefinition() throws IOException {
        io.serverlessworkflow.api.types.Workflow workflowDef = parser.parseFromClasspath("emit.yaml");

        ServerlessWorkflowExecutor executor = new ServerlessWorkflowExecutor(workflowDef);
        assertNotNull(executor);

        WorkflowStub stub = executor.create();
        assertNotNull(stub);
    }

    @Test
    void shouldCreateWorkflowFromListenDefinition() throws IOException {
        io.serverlessworkflow.api.types.Workflow workflowDef = parser.parseFromClasspath("listen.yaml");

        ServerlessWorkflowExecutor executor = new ServerlessWorkflowExecutor(workflowDef);
        assertNotNull(executor);

        WorkflowStub stub = executor.create();
        assertNotNull(stub);
    }
}
