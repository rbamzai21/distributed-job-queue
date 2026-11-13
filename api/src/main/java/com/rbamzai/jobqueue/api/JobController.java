package com.rbamzai.jobqueue.api;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobStatus;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private static final int MAX_LIST_LIMIT = 500;
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 255;

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /**
     * Enqueues a job. With an {@code Idempotency-Key} header, retrying the same request (after a timeout,
     * say) returns the original job with 200 instead of creating a duplicate.
     */
    @PostMapping
    public ResponseEntity<Job> enqueue(
            @Valid @RequestBody EnqueueRequest request,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey) {
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Idempotency-Key must be 1-" + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }
        JobService.EnqueueResult result = service.enqueue(request, idempotencyKey);
        if (!result.created()) {
            return ResponseEntity.ok(result.job());
        }
        return ResponseEntity.created(URI.create("/jobs/" + result.job().id())).body(result.job());
    }

    @GetMapping("/{id}")
    public Job get(@PathVariable UUID id) {
        return service.get(id);
    }

    @GetMapping
    public List<Job> list(
            @RequestParam(required = false) JobStatus status, @RequestParam(defaultValue = "50") int limit) {
        return service.list(status, Math.clamp(limit, 1, MAX_LIST_LIMIT));
    }

    @PostMapping("/{id}/cancel")
    public Job cancel(@PathVariable UUID id) {
        return service.cancel(id);
    }

    /** Reruns a job from the dead-letter queue. */
    @PostMapping("/{id}/retry")
    public Job retry(@PathVariable UUID id) {
        return service.retry(id);
    }
}
