package com.rbamzai.jobqueue.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobStatus;
import com.rbamzai.jobqueue.common.ReadyQueue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class JobService {

    static final int DEFAULT_PRIORITY = 0;
    static final int DEFAULT_MAX_ATTEMPTS = 5;

    private final JobRepository repository;
    private final ReadyQueue readyQueue;
    private final Counter created;
    private final Counter replayed;

    public JobService(JobRepository repository, ReadyQueue readyQueue, MeterRegistry registry) {
        this.repository = repository;
        this.readyQueue = readyQueue;
        this.created = enqueueCounter(registry, "created");
        this.replayed = enqueueCounter(registry, "replayed");
    }

    private static Counter enqueueCounter(MeterRegistry registry, String result) {
        return Counter.builder("jobqueue.jobs.enqueued")
                .description("POST /jobs results; replayed means an Idempotency-Key matched an existing job")
                .tag("result", result)
                .register(registry);
    }

    /** {@code created} is false when an earlier request with the same idempotency key already made the job. */
    public record EnqueueResult(Job job, boolean created) {}

    public EnqueueResult enqueue(EnqueueRequest request, String idempotencyKey) {
        JsonNode payload = Objects.requireNonNullElse(request.payload(), JsonNodeFactory.instance.objectNode());
        int priority = Objects.requireNonNullElse(request.priority(), DEFAULT_PRIORITY);
        int maxAttempts = Objects.requireNonNullElse(request.maxAttempts(), DEFAULT_MAX_ATTEMPTS);
        long delaySeconds = Objects.requireNonNullElse(request.delaySeconds(), 0L);

        Optional<Job> inserted =
                repository.insert(request.type(), payload.toString(), priority, maxAttempts, delaySeconds, idempotencyKey);
        if (inserted.isPresent()) {
            created.increment();
            // After the insert has committed: a worker that pops the id must be able to see the row.
            readyQueue.offer(inserted.get());
            return new EnqueueResult(inserted.get(), true);
        }

        // The key was used before. A retry of the same request gets the original job back; reusing the
        // key for a different job is a client bug, so say so instead of silently returning the old one.
        Job existing = repository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new IllegalStateException("insert conflicted but no job has key " + idempotencyKey));
        boolean sameRequest = existing.type().equals(request.type())
                && existing.payload().equals(payload)
                && existing.priority() == priority
                && existing.maxAttempts() == maxAttempts;
        if (!sameRequest) {
            throw new JobConflictException("Idempotency-Key '" + idempotencyKey + "' was already used for a different job");
        }
        replayed.increment();
        return new EnqueueResult(existing, false);
    }

    public Job get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }

    public List<Job> list(JobStatus status, int limit) {
        return status == null ? repository.findRecent(limit) : repository.findRecentByStatus(status, limit);
    }

    public Job cancel(UUID id) {
        return repository.cancelIfPending(id)
                .orElseThrow(() -> new JobConflictException("only PENDING jobs can be cancelled; job is " + get(id).status()));
    }

    public Job retry(UUID id) {
        Job requeued = repository.requeueIfDead(id)
                .orElseThrow(() -> new JobConflictException("only DEAD jobs can be retried; job is " + get(id).status()));
        readyQueue.offer(requeued);
        return requeued;
    }
}
