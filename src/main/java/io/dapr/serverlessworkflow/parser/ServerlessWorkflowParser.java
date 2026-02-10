package io.dapr.serverlessworkflow.parser;

import io.serverlessworkflow.api.WorkflowFormat;
import io.serverlessworkflow.api.WorkflowReader;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for Serverless Workflow definitions.
 * Uses the serverlessworkflow-api library to parse YAML/JSON workflow definitions.
 */
@Component
public class ServerlessWorkflowParser {

    private static final Logger logger = LoggerFactory.getLogger(ServerlessWorkflowParser.class);

    /**
     * Parse a single workflow definition from an input stream.
     *
     * @param inputStream the input stream containing the workflow definition
     * @param format the workflow format (YAML or JSON)
     * @return the parsed Workflow object
     * @throws IOException if parsing fails
     */
    public Workflow parse(InputStream inputStream, WorkflowFormat format) throws IOException {
        byte[] bytes = inputStream.readAllBytes();
        return WorkflowReader.readWorkflow(bytes, format);
    }

    /**
     * Parse a workflow definition from a classpath resource.
     *
     * @param resourcePath the classpath resource path (e.g., "startwars.yaml")
     * @return the parsed Workflow object
     * @throws IOException if the resource is not found or parsing fails
     */
    public Workflow parseFromClasspath(String resourcePath) throws IOException {
        logger.info("Parsing workflow from classpath: {}", resourcePath);
        return WorkflowReader.readWorkflowFromClasspath(resourcePath);
    }

    /**
     * Parse all workflow definitions matching a pattern from the classpath.
     *
     * @param pattern the resource pattern (e.g., "classpath:*.yaml")
     * @return list of parsed Workflow objects
     * @throws IOException if scanning or parsing fails
     */
    public List<Workflow> parseAllFromPattern(String pattern) throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources(pattern);

        List<Workflow> workflows = new ArrayList<>();
        for (Resource resource : resources) {
            String filename = resource.getFilename();
            if (filename == null) {
                continue;
            }

            // Skip application.yaml and other non-workflow files
            if (filename.equals("application.yaml") || filename.equals("application.yml")) {
                continue;
            }

            try (InputStream is = resource.getInputStream()) {
                logger.info("Parsing workflow from: {}", filename);
                WorkflowFormat format = filename.endsWith(".json") ? WorkflowFormat.JSON : WorkflowFormat.YAML;
                Workflow workflow = parse(is, format);
                workflows.add(workflow);
                logger.info("Successfully parsed workflow: {}.{}",
                    workflow.getDocument().getNamespace(),
                    workflow.getDocument().getName());
            } catch (Exception e) {
                logger.warn("Failed to parse workflow from {}: {}", filename, e.getMessage());
            }
        }
        return workflows;
    }

    /**
     * Parse all YAML workflow definitions from the classpath root.
     *
     * @return list of parsed Workflow objects
     * @throws IOException if scanning or parsing fails
     */
    public List<Workflow> parseAllYamlWorkflows() throws IOException {
        return parseAllFromPattern("classpath:*.yaml");
    }
}
