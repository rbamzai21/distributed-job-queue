package com.rbamzai.jobqueue.common;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/** An immutable snapshot of one row of the {@code jobs} table. Postgres is the source of truth. */
public record Job(
        UUID id,
        String type,
        JsonNode payload,
        int priority,
        JobStatus status,
        int attempts,
        int maxAttempts,
        Instant runAt,
        Instant leaseExpiresAt,
        UUID leaseId,
        String workerId,
        String idempotencyKey,
        String lastError,
        Instant createdAt,
        Instant updatedAt) {}
