package com.rbamzai.jobqueue.worker.handler;

import com.rbamzai.jobqueue.common.Job;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Simulates sending an email: an I/O-bound job that mostly waits on the network.
 * Payload: {"to": "...", "latencyMs": 200} (latencyMs optional).
 *
 * <p>A real implementation would pass the job id to the email provider as an idempotency key, so a
 * rerun after a crash doesn't send the email twice.
 */
@Component
public class EmailHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(EmailHandler.class);
    private static final long MAX_LATENCY_MS = 600_000;

    @Override
    public String type() {
        return "email";
    }

    @Override
    public void handle(Job job) throws InterruptedException {
        String to = job.payload().path("to").asText("");
        if (to.isBlank()) {
            throw new NonRetryableJobException("payload.to is required");
        }
        // Stand-in for the SMTP/provider round trip. On a virtual thread this sleep frees the carrier thread.
        // payload.latencyMs pins the delay, which crash and shutdown tests use to keep a job in flight.
        long latencyMs = job.payload().has("latencyMs")
                ? job.payload().get("latencyMs").asLong()
                : ThreadLocalRandom.current().nextLong(50, 250);
        if (latencyMs < 0 || latencyMs > MAX_LATENCY_MS) {
            throw new NonRetryableJobException("payload.latencyMs must be between 0 and " + MAX_LATENCY_MS);
        }
        Thread.sleep(latencyMs);
        log.debug("Sent email for job {} to {}", job.id(), to);
    }
}
