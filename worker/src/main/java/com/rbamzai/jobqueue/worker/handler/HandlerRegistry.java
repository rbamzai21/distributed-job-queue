package com.rbamzai.jobqueue.worker.handler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** Looks up the handler for a job type. Fails at startup if two handlers claim the same type. */
@Component
public class HandlerRegistry {

    private final Map<String, JobHandler> handlersByType;

    public HandlerRegistry(List<JobHandler> handlers) {
        this.handlersByType = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(JobHandler::type, Function.identity()));
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlersByType.get(type));
    }
}
