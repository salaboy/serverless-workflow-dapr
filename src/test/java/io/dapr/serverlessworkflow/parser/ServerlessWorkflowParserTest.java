package io.dapr.serverlessworkflow.parser;

import io.serverlessworkflow.api.types.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the ServerlessWorkflowParser.
 */
class ServerlessWorkflowParserTest {

    private ServerlessWorkflowParser parser;

    @BeforeEach
    void setUp() {
        parser = new ServerlessWorkflowParser();
    }

    @Test
    void shouldParseStarWarsWorkflow() throws IOException {
        Workflow workflow = parser.parseFromClasspath("startwars.yaml");

        assertNotNull(workflow);
        assertEquals("examples", workflow.getDocument().getNamespace());
        assertEquals("star-wars-homeplanet", workflow.getDocument().getName());
        assertEquals("1.0.0", workflow.getDocument().getVersion());

        // Check that there are tasks in the 'do' block
        List<TaskItem> tasks = workflow.getDo();
        assertNotNull(tasks);
        assertFalse(tasks.isEmpty());

        // Should have 2 HTTP call tasks
        assertEquals(2, tasks.size());

        // First task: getStarWarsCharacter
        TaskItem firstTask = tasks.get(0);
        assertEquals("getStarWarsCharacter", firstTask.getName());
        assertNotNull(firstTask.getTask().getCallTask());

        // Second task: getStarWarsHomeworld
        TaskItem secondTask = tasks.get(1);
        assertEquals("getStarWarsHomeworld", secondTask.getName());
        assertNotNull(secondTask.getTask().getCallTask());
    }

    @Test
    void shouldParseGetWorkflow() throws IOException {
        Workflow workflow = parser.parseFromClasspath("get.yaml");

        assertNotNull(workflow);
        assertEquals("examples", workflow.getDocument().getNamespace());
        assertEquals("call-http-shorthand-endpoint", workflow.getDocument().getName());

        List<TaskItem> tasks = workflow.getDo();
        assertNotNull(tasks);
        assertEquals(1, tasks.size());

        TaskItem task = tasks.get(0);
        assertEquals("getPet", task.getName());

        // Verify it's an HTTP call
        CallTask callTask = task.getTask().getCallTask();
        assertNotNull(callTask);
        assertNotNull(callTask.getCallHTTP());
    }

    @Test
    void shouldParseEmitWorkflow() throws IOException {
        Workflow workflow = parser.parseFromClasspath("emit.yaml");

        assertNotNull(workflow);
        assertEquals("test", workflow.getDocument().getNamespace());
        assertEquals("emit", workflow.getDocument().getName());

        List<TaskItem> tasks = workflow.getDo();
        assertNotNull(tasks);
        assertEquals(1, tasks.size());

        TaskItem task = tasks.get(0);
        assertEquals("emitEvent", task.getName());

        // Verify it's an emit task
        EmitTask emitTask = task.getTask().getEmitTask();
        assertNotNull(emitTask);
        assertNotNull(emitTask.getEmit());
    }

    @Test
    void shouldParseListenWorkflow() throws IOException {
        Workflow workflow = parser.parseFromClasspath("listen.yaml");

        assertNotNull(workflow);
        assertEquals("examples", workflow.getDocument().getNamespace());
        assertEquals("listen", workflow.getDocument().getName());

        List<TaskItem> tasks = workflow.getDo();
        assertNotNull(tasks);
        assertEquals(1, tasks.size());

        TaskItem task = tasks.get(0);
        assertEquals("callDoctor", task.getName());

        // Verify it's a listen task
        ListenTask listenTask = task.getTask().getListenTask();
        assertNotNull(listenTask);
        assertNotNull(listenTask.getListen());
    }

    @Test
    void shouldParseMultipleWorkflows() throws IOException {
        // Parse individual workflows instead of using pattern matching
        List<String> workflowFiles = List.of("startwars.yaml", "get.yaml", "emit.yaml", "listen.yaml");

        for (String file : workflowFiles) {
            Workflow workflow = parser.parseFromClasspath(file);
            assertNotNull(workflow, "Failed to parse: " + file);
            assertNotNull(workflow.getDocument());
            assertNotNull(workflow.getDocument().getName());
            assertNotNull(workflow.getDocument().getNamespace());
        }
    }
}
