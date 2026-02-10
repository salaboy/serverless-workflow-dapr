package io.dapr.serverlessworkflow.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dapr.serverlessworkflow.activities.EmitEventActivity;
import io.dapr.serverlessworkflow.activities.HttpCallActivity;
import io.dapr.serverlessworkflow.activities.WaitForEventActivity;
import io.dapr.workflows.Workflow;
import io.dapr.workflows.WorkflowContext;
import io.dapr.workflows.WorkflowStub;
import io.serverlessworkflow.api.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes a Serverless Workflow definition as a Dapr Workflow.
 * This class translates Serverless Workflow tasks into Dapr workflow activity calls.
 */
public class ServerlessWorkflowExecutor implements Workflow {

    private static final Logger logger = LoggerFactory.getLogger(ServerlessWorkflowExecutor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Pattern EXPRESSION_PATTERN = Pattern.compile("\\$\\{\\s*([^}]+)\\s*}");
    private static final Pattern CONTEXT_PATTERN = Pattern.compile("\\$context\\.([\\w.]+)");
    private static final Pattern INPUT_PATTERN = Pattern.compile("\\.([\\w.]+)");

    private final io.serverlessworkflow.api.types.Workflow workflowDefinition;

    public ServerlessWorkflowExecutor(io.serverlessworkflow.api.types.Workflow workflowDefinition) {
        this.workflowDefinition = workflowDefinition;
    }

    @Override
    public WorkflowStub create() {
        return ctx -> {
            logger.info("Starting workflow: {}.{}",
                workflowDefinition.getDocument().getNamespace(),
                workflowDefinition.getDocument().getName());

            // Get initial input
            Map<String, Object> workflowContext = new HashMap<>();
            Object input = ctx.getInput(Object.class);
            if (input != null) {
                workflowContext.put("input", input);
            }

            // Execute the 'do' block tasks
            List<TaskItem> doTasks = workflowDefinition.getDo();
            if (doTasks != null && !doTasks.isEmpty()) {
                Object result = executeTasks(ctx, doTasks, workflowContext);
                workflowContext.put("result", result);
            }

            logger.info("Workflow completed: {}.{}",
                workflowDefinition.getDocument().getNamespace(),
                workflowDefinition.getDocument().getName());

            // WorkflowStub.run() is void, so we don't return anything
            // The result is stored in workflowContext for retrieval via output
        };
    }

    private Object executeTasks(WorkflowContext ctx, List<TaskItem> tasks, Map<String, Object> workflowContext) {
        Object lastResult = null;

        for (TaskItem taskItem : tasks) {
            String taskName = taskItem.getName();
            Task task = taskItem.getTask();

            logger.info("Executing task: {}", taskName);

            lastResult = executeTask(ctx, taskName, task, workflowContext);

            // Store the result with the task name for later reference
            workflowContext.put(taskName, lastResult);
        }

        return lastResult;
    }

    private Object executeTask(WorkflowContext ctx, String taskName, Task task, Map<String, Object> workflowContext) {
        // Handle CallTask (HTTP, gRPC, OpenAPI, etc.)
        if (task.getCallTask() != null) {
            return executeCallTask(ctx, taskName, task.getCallTask(), workflowContext);
        }

        // Handle EmitTask
        if (task.getEmitTask() != null) {
            return executeEmitTask(ctx, taskName, task.getEmitTask(), workflowContext);
        }

        // Handle ListenTask
        if (task.getListenTask() != null) {
            return executeListenTask(ctx, taskName, task.getListenTask(), workflowContext);
        }

        // Handle DoTask (nested tasks)
        if (task.getDoTask() != null) {
            return executeTasks(ctx, task.getDoTask().getDo(), workflowContext);
        }

        // Handle ForkTask (parallel execution)
        if (task.getForkTask() != null) {
            return executeForkTask(ctx, taskName, task.getForkTask(), workflowContext);
        }

        // Handle SwitchTask (conditional)
        if (task.getSwitchTask() != null) {
            return executeSwitchTask(ctx, taskName, task.getSwitchTask(), workflowContext);
        }

        // Handle WaitTask
        if (task.getWaitTask() != null) {
            return executeWaitTask(ctx, taskName, task.getWaitTask(), workflowContext);
        }

        logger.warn("Unknown task type for task: {}", taskName);
        return null;
    }

    private Object executeCallTask(WorkflowContext ctx, String taskName, CallTask callTask, Map<String, Object> workflowContext) {
        // Check if it's an HTTP call
        if (callTask.getCallHTTP() != null) {
            return executeHttpCall(ctx, taskName, callTask.getCallHTTP(), workflowContext);
        }

        // Check if it's an AsyncAPI call
        if (callTask.getCallAsyncAPI() != null) {
            logger.info("AsyncAPI call not yet implemented for task: {}", taskName);
            return null;
        }

        // Check if it's a gRPC call
        if (callTask.getCallGRPC() != null) {
            logger.info("gRPC call not yet implemented for task: {}", taskName);
            return null;
        }

        // Check if it's an OpenAPI call
        if (callTask.getCallOpenAPI() != null) {
            logger.info("OpenAPI call not yet implemented for task: {}", taskName);
            return null;
        }

        // Check if it's a function call
        if (callTask.getCallFunction() != null) {
            logger.info("Function call not yet implemented for task: {}", taskName);
            return null;
        }

        logger.warn("Unknown call type for task: {}", taskName);
        return null;
    }

    private Object executeHttpCall(WorkflowContext ctx, String taskName, CallHTTP httpCall, Map<String, Object> workflowContext) {
        HTTPArguments httpArgs = httpCall.getWith();
        if (httpArgs == null) {
            logger.warn("HTTP call missing 'with' configuration for task: {}", taskName);
            return null;
        }

        String method = httpArgs.getMethod() != null ? httpArgs.getMethod() : "GET";
        String endpoint = resolveEndpoint(httpArgs.getEndpoint(), workflowContext);

        logger.info("HTTP {} to {}", method, endpoint);

        HttpCallActivity.HttpCallInput input = new HttpCallActivity.HttpCallInput(
            method,
            endpoint,
            null,
            null
        );

        HttpCallActivity.HttpCallOutput output = ctx.callActivity(
            HttpCallActivity.class.getName(),
            input,
            HttpCallActivity.HttpCallOutput.class
        ).await();

        // Handle export if defined (from TaskBase)
        Export export = httpCall.getExport();
        if (export != null && export.getAs() != null) {
            processExport(export.getAs(), output, workflowContext);
        }

        return output;
    }

    private String resolveEndpoint(Endpoint endpoint, Map<String, Object> workflowContext) {
        if (endpoint == null) {
            return "";
        }

        String endpointUri = null;

        // Handle UriTemplate
        if (endpoint.getUriTemplate() != null) {
            UriTemplate uriTemplate = endpoint.getUriTemplate();
            if (uriTemplate.getLiteralUri() != null) {
                endpointUri = uriTemplate.getLiteralUri().toString();
            } else if (uriTemplate.getLiteralUriTemplate() != null) {
                endpointUri = uriTemplate.getLiteralUriTemplate();
            }
        }

        // Handle RuntimeExpression
        if (endpoint.getRuntimeExpression() != null) {
            endpointUri = endpoint.getRuntimeExpression();
        }

        // Handle EndpointConfiguration
        if (endpoint.getEndpointConfiguration() != null) {
            EndpointConfiguration config = endpoint.getEndpointConfiguration();
            if (config.getUri() != null) {
                EndpointUri endpointUriObj = config.getUri();
                if (endpointUriObj.getLiteralEndpointURI() != null) {
                    UriTemplate uriTemplate = endpointUriObj.getLiteralEndpointURI();
                    if (uriTemplate.getLiteralUri() != null) {
                        endpointUri = uriTemplate.getLiteralUri().toString();
                    } else if (uriTemplate.getLiteralUriTemplate() != null) {
                        endpointUri = uriTemplate.getLiteralUriTemplate();
                    }
                } else if (endpointUriObj.getExpressionEndpointURI() != null) {
                    endpointUri = endpointUriObj.getExpressionEndpointURI();
                }
            }
        }

        if (endpointUri == null) {
            return "";
        }

        // Resolve expressions in the endpoint
        return resolveExpression(endpointUri, workflowContext);
    }

    private String resolveExpression(String expression, Map<String, Object> workflowContext) {
        if (expression == null) {
            return "";
        }

        // Handle ${...} expressions
        Matcher matcher = EXPRESSION_PATTERN.matcher(expression);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String expr = matcher.group(1).trim();
            String replacement = evaluateExpression(expr, workflowContext);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);

        // Handle {variable} style path parameters
        String resolved = result.toString();
        for (Map.Entry<String, Object> entry : workflowContext.entrySet()) {
            if (entry.getValue() != null) {
                resolved = resolved.replace("{" + entry.getKey() + "}", entry.getValue().toString());
            }
        }

        // Also check in input if present
        Object input = workflowContext.get("input");
        if (input instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> inputMap = (Map<String, Object>) input;
            for (Map.Entry<String, Object> entry : inputMap.entrySet()) {
                if (entry.getValue() != null) {
                    resolved = resolved.replace("{" + entry.getKey() + "}", entry.getValue().toString());
                }
            }
        }

        return resolved;
    }

    private String evaluateExpression(String expr, Map<String, Object> workflowContext) {
        // Handle $context.xxx references
        Matcher contextMatcher = CONTEXT_PATTERN.matcher(expr);
        if (contextMatcher.find()) {
            String path = contextMatcher.group(1);
            Object value = getValueByPath(workflowContext, path);
            return value != null ? value.toString() : "";
        }

        // Handle .xxx references (from input/current context)
        Matcher inputMatcher = INPUT_PATTERN.matcher(expr);
        if (inputMatcher.matches()) {
            String path = inputMatcher.group(1);
            Object input = workflowContext.get("input");
            if (input != null) {
                Object value = getValueByPath(input, path);
                return value != null ? value.toString() : "";
            }
        }

        // Direct context lookup
        Object value = workflowContext.get(expr);
        return value != null ? value.toString() : expr;
    }

    private Object getValueByPath(Object obj, String path) {
        if (obj == null || path == null || path.isEmpty()) {
            return null;
        }

        String[] parts = path.split("\\.");
        Object current = obj;

        for (String part : parts) {
            if (current == null) {
                return null;
            }

            if (current instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) current;
                current = map.get(part);
            } else if (current instanceof JsonNode) {
                JsonNode node = (JsonNode) current;
                current = node.get(part);
                if (current != null && ((JsonNode) current).isValueNode()) {
                    return ((JsonNode) current).asText();
                }
            } else {
                // Try reflection or return null
                return null;
            }
        }

        return current;
    }

    private void processExport(ExportAs exportAs, Object output, Map<String, Object> workflowContext) {
        if (exportAs == null) {
            return;
        }

        // ExportAs can be an object or a string
        Object asValue = exportAs.getObject();
        if (asValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> asMap = (Map<String, Object>) asValue;
            for (Map.Entry<String, Object> entry : asMap.entrySet()) {
                String key = entry.getKey();
                Object valueExpr = entry.getValue();

                if (valueExpr instanceof String) {
                    String expr = (String) valueExpr;
                    // Evaluate the expression against the output
                    Object resolvedValue = evaluateExportExpression(expr, output, workflowContext);
                    workflowContext.put(key, resolvedValue);
                    logger.debug("Exported {} = {}", key, resolvedValue);
                }
            }
        }
    }

    private Object evaluateExportExpression(String expr, Object output, Map<String, Object> workflowContext) {
        // Handle ${.content.xxx} style expressions
        Matcher matcher = EXPRESSION_PATTERN.matcher(expr);
        if (matcher.find()) {
            String innerExpr = matcher.group(1).trim();
            if (innerExpr.startsWith(".")) {
                innerExpr = innerExpr.substring(1);
            }

            // Parse output to extract nested values
            if (output instanceof HttpCallActivity.HttpCallOutput httpOutput) {
                if (innerExpr.startsWith("content.")) {
                    String contentPath = innerExpr.substring("content.".length());
                    try {
                        JsonNode contentNode = objectMapper.readTree(httpOutput.content());
                        Object value = getValueByPath(contentNode, contentPath);
                        return value;
                    } catch (JsonProcessingException e) {
                        logger.warn("Failed to parse HTTP response content as JSON", e);
                        return null;
                    }
                }
            }

            return getValueByPath(output, innerExpr);
        }

        return expr;
    }

    private Object executeEmitTask(WorkflowContext ctx, String taskName, EmitTask emitTask, Map<String, Object> workflowContext) {
        EmitTaskConfiguration eventConfig = emitTask.getEmit();
        if (eventConfig == null || eventConfig.getEvent() == null) {
            logger.warn("Emit task missing event configuration for task: {}", taskName);
            return null;
        }

        EmitEventDefinition eventDef = eventConfig.getEvent();
        EventProperties eventProps = eventDef.getWith();
        if (eventProps == null) {
            logger.warn("Emit task missing event properties for task: {}", taskName);
            return null;
        }

        String type = eventProps.getType();
        String source = "unknown";
        if (eventProps.getSource() != null) {
            EventSource eventSource = eventProps.getSource();
            if (eventSource.getUriTemplate() != null) {
                UriTemplate uriTemplate = eventSource.getUriTemplate();
                if (uriTemplate.getLiteralUri() != null) {
                    source = uriTemplate.getLiteralUri().toString();
                } else if (uriTemplate.getLiteralUriTemplate() != null) {
                    source = uriTemplate.getLiteralUriTemplate();
                }
            } else if (eventSource.getRuntimeExpression() != null) {
                source = resolveExpression(eventSource.getRuntimeExpression(), workflowContext);
            }
        }

        // Resolve data expressions
        Map<String, Object> data = new HashMap<>();
        if (eventProps.getData() != null) {
            EventData eventData = eventProps.getData();
            if (eventData.getObject() != null) {
                data = resolveDataExpressions(eventData.getObject(), workflowContext);
            } else if (eventData.getRuntimeExpression() != null) {
                String resolved = resolveExpression(eventData.getRuntimeExpression(), workflowContext);
                data.put("value", resolved);
            }
        }

        EmitEventActivity.EmitEventInput input = new EmitEventActivity.EmitEventInput(type, source, data);

        return ctx.callActivity(
            EmitEventActivity.class.getName(),
            input,
            EmitEventActivity.EmitEventOutput.class
        ).await();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> resolveDataExpressions(Object dataObj, Map<String, Object> workflowContext) {
        Map<String, Object> resolved = new HashMap<>();

        if (dataObj instanceof Map) {
            Map<String, Object> dataMap = (Map<String, Object>) dataObj;
            for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof String) {
                    resolved.put(entry.getKey(), resolveExpression((String) value, workflowContext));
                } else {
                    resolved.put(entry.getKey(), value);
                }
            }
        }

        return resolved;
    }

    private Object executeListenTask(WorkflowContext ctx, String taskName, ListenTask listenTask, Map<String, Object> workflowContext) {
        ListenTaskConfiguration listenConfig = listenTask.getListen();
        if (listenConfig == null || listenConfig.getTo() == null) {
            logger.warn("Listen task missing configuration for task: {}", taskName);
            return null;
        }

        ListenTo listenTo = listenConfig.getTo();
        String eventType = extractEventType(listenTo);

        if (eventType != null) {
            logger.info("Waiting for external event of type: {}", eventType);

            // Use Dapr's waitForExternalEvent
            Object eventData = ctx.waitForExternalEvent(eventType, Duration.ofHours(24), Object.class).await();

            logger.info("Received external event: {}", eventType);
            return eventData;
        }

        // Fallback to activity-based approach
        WaitForEventActivity.WaitForEventInput input = new WaitForEventActivity.WaitForEventInput(
            "unknown",
            null
        );

        return ctx.callActivity(
            WaitForEventActivity.class.getName(),
            input,
            WaitForEventActivity.WaitForEventOutput.class
        ).await();
    }

    private String extractEventType(ListenTo listenTo) {
        // Check for 'one' event configuration
        if (listenTo.getOneEventConsumptionStrategy() != null) {
            OneEventConsumptionStrategy one = listenTo.getOneEventConsumptionStrategy();
            if (one.getOne() != null && one.getOne().getWith() != null) {
                return one.getOne().getWith().getType();
            }
        }

        // Check for 'all' events configuration
        if (listenTo.getAllEventConsumptionStrategy() != null) {
            AllEventConsumptionStrategy all = listenTo.getAllEventConsumptionStrategy();
            if (all.getAll() != null && !all.getAll().isEmpty()) {
                EventFilter first = all.getAll().get(0);
                if (first.getWith() != null) {
                    return first.getWith().getType();
                }
            }
        }

        // Check for 'any' events configuration
        if (listenTo.getAnyEventConsumptionStrategy() != null) {
            AnyEventConsumptionStrategy any = listenTo.getAnyEventConsumptionStrategy();
            if (any.getAny() != null && !any.getAny().isEmpty()) {
                EventFilter first = any.getAny().get(0);
                if (first.getWith() != null) {
                    return first.getWith().getType();
                }
            }
        }

        return null;
    }

    private Object executeForkTask(WorkflowContext ctx, String taskName, ForkTask forkTask, Map<String, Object> workflowContext) {
        ForkTaskConfiguration forkConfig = forkTask.getFork();
        if (forkConfig == null || forkConfig.getBranches() == null) {
            logger.warn("Fork task missing branches for task: {}", taskName);
            return null;
        }

        // Note: True parallel execution in Dapr Workflows requires using Task.WhenAll
        // For simplicity, we execute branches sequentially here
        // A more sophisticated implementation would use ctx.allOf()
        Map<String, Object> branchResults = new HashMap<>();

        List<TaskItem> branches = forkConfig.getBranches();
        for (TaskItem branch : branches) {
            String branchName = branch.getName();
            Task branchTask = branch.getTask();

            logger.info("Executing fork branch: {}", branchName);

            // Execute the branch task
            Object result = executeTask(ctx, branchName, branchTask, workflowContext);
            branchResults.put(branchName, result);
        }

        return branchResults;
    }

    private Object executeSwitchTask(WorkflowContext ctx, String taskName, SwitchTask switchTask, Map<String, Object> workflowContext) {
        List<SwitchItem> switchItems = switchTask.getSwitch();
        if (switchItems == null || switchItems.isEmpty()) {
            logger.warn("Switch task missing cases for task: {}", taskName);
            return null;
        }

        // Evaluate each case condition
        for (SwitchItem switchItem : switchItems) {
            SwitchCase switchCase = switchItem.getSwitchCase();
            if (switchCase == null) {
                continue;
            }

            String condition = switchCase.getWhen();
            if (condition != null && evaluateCondition(condition, workflowContext)) {
                logger.info("Switch case matched: {}", condition);
                // Execute the case's then task
                if (switchCase.getThen() != null) {
                    FlowDirective directive = switchCase.getThen();
                    // Handle flow directive (continue, exit, etc.)
                    return handleFlowDirective(directive, workflowContext);
                }
            }
        }

        return null;
    }

    private boolean evaluateCondition(String condition, Map<String, Object> workflowContext) {
        // Simple condition evaluation - in production, use a proper expression engine
        String resolved = resolveExpression(condition, workflowContext);
        return Boolean.parseBoolean(resolved) || "true".equalsIgnoreCase(resolved);
    }

    private Object handleFlowDirective(FlowDirective directive, Map<String, Object> workflowContext) {
        // Handle flow directives like continue, exit, etc.
        return null;
    }

    private Object executeWaitTask(WorkflowContext ctx, String taskName, WaitTask waitTask, Map<String, Object> workflowContext) {
        TimeoutAfter timeoutAfter = waitTask.getWait();
        if (timeoutAfter != null) {
            Duration duration = parseDuration(timeoutAfter);
            if (duration != null) {
                logger.info("Waiting for duration: {}", duration);
                ctx.createTimer(duration).await();
                return "waited";
            }
        }
        return null;
    }

    private Duration parseDuration(TimeoutAfter timeoutAfter) {
        // Check for inline duration
        if (timeoutAfter.getDurationInline() != null) {
            DurationInline inline = timeoutAfter.getDurationInline();
            // DurationInline contains days, hours, minutes, seconds, milliseconds
            long totalMillis = 0;
            // Need to extract individual components - for now, use a simple approach
            return Duration.ofSeconds(60); // Default to 60 seconds if not parseable
        }

        // Check for literal duration string (ISO 8601 format)
        String literal = timeoutAfter.getDurationLiteral();
        if (literal != null) {
            try {
                return Duration.parse(literal);
            } catch (Exception e) {
                logger.warn("Failed to parse duration: {}", literal);
            }
        }

        // Check for expression
        if (timeoutAfter.getDurationExpression() != null) {
            // Would need to evaluate the expression
            return Duration.ofSeconds(60);
        }

        return null;
    }
}
