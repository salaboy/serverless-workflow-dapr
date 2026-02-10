package io.dapr.serverlessworkflow.config;

import io.dapr.client.DaprClient;
import io.dapr.client.DaprClientBuilder;
import io.dapr.serverlessworkflow.activities.EmitEventActivity;
import io.dapr.serverlessworkflow.activities.HttpCallActivity;
import io.dapr.serverlessworkflow.activities.WaitForEventActivity;
import io.dapr.serverlessworkflow.parser.ServerlessWorkflowParser;
import io.dapr.serverlessworkflow.workflow.DynamicWorkflowFactory;
import io.dapr.serverlessworkflow.workflow.ServerlessWorkflowRegistry;
import io.dapr.workflows.client.DaprWorkflowClient;
import io.dapr.workflows.runtime.WorkflowRuntime;
import io.dapr.workflows.runtime.WorkflowRuntimeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * Spring Boot auto-configuration for Serverless Workflow to Dapr integration.
 */
@Configuration
public class ServerlessWorkflowAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ServerlessWorkflowAutoConfiguration.class);

    @Value("${dapr.pubsub.name:pubsub}")
    private String pubsubName;

    @Bean
    @ConditionalOnMissingBean
    public DaprClient daprClient() {
        return new DaprClientBuilder().build();
    }

    @Bean
    @ConditionalOnMissingBean
    public DaprWorkflowClient daprWorkflowClient() {
        return new DaprWorkflowClient();
    }

    @Bean
    @ConditionalOnMissingBean
    public ServerlessWorkflowParser serverlessWorkflowParser() {
        return new ServerlessWorkflowParser();
    }

    @Bean
    @ConditionalOnMissingBean
    public ServerlessWorkflowRegistry serverlessWorkflowRegistry(ServerlessWorkflowParser parser) {
        return new ServerlessWorkflowRegistry(parser);
    }

    @Bean
    @ConditionalOnMissingBean
    public DynamicWorkflowFactory dynamicWorkflowFactory(ServerlessWorkflowRegistry registry) {
        return new DynamicWorkflowFactory(registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public HttpCallActivity httpCallActivity() {
        return new HttpCallActivity();
    }

    @Bean
    @ConditionalOnMissingBean
    public EmitEventActivity emitEventActivity(DaprClient daprClient) {
        return new EmitEventActivity(daprClient, pubsubName);
    }

    @Bean
    @ConditionalOnMissingBean
    public WaitForEventActivity waitForEventActivity() {
        return new WaitForEventActivity();
    }

    @Bean
    public WorkflowRuntime workflowRuntime(ServerlessWorkflowRegistry registry,
                                           HttpCallActivity httpCallActivity,
                                           EmitEventActivity emitEventActivity,
                                           WaitForEventActivity waitForEventActivity) {
        WorkflowRuntimeBuilder builder = new WorkflowRuntimeBuilder();

        // Register activities
        builder.registerActivity(httpCallActivity);
        builder.registerActivity(emitEventActivity);
        builder.registerActivity(waitForEventActivity);

        // Register each workflow from the registry
        registry.getAllExecutors().forEach((workflowId, executor) -> {
            logger.info("Registering Dapr workflow: {}", workflowId);
            builder.registerWorkflow(executor);
        });

        return builder.build();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startWorkflowRuntime(ApplicationReadyEvent event) {
        WorkflowRuntime runtime = event.getApplicationContext().getBean(WorkflowRuntime.class);
        logger.info("Starting Dapr Workflow Runtime...");
        runtime.start(false);
        logger.info("Dapr Workflow Runtime started");
    }
}
