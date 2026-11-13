package com.rbamzai.jobqueue.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;

/** Body of {@code POST /jobs}. Everything except {@code type} is optional. */
public record EnqueueRequest(
        @NotBlank String type,
        JsonNode payload,
        Integer priority,
        @Min(1) @Max(100) Integer maxAttempts,
        @PositiveOrZero Long delaySeconds) {}
