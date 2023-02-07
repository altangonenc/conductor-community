package com.netflix.conductor.contribs.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "conductor.workflow-status-listener.type", havingValue = "multiple")
public class QueueStatusPublisherAndArchiveConfiguration {

    @Bean
    public WorkflowStatusListener getWorkflowStatusListener(
            EventQueues eventQueues,
            ExecutionDAOFacade executionDAOFacade,
            ObjectMapper objectMapper) {
        return new QueueStatusPublisherAndArchive(eventQueues, executionDAOFacade, objectMapper);
    }
}