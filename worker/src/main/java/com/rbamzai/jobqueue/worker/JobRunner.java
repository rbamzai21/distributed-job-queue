package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.Backoff;
import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.worker.WorkerMetrics.Outcome;
import com.rbamzai.jobqueue.worker.handler.HandlerRegistry;
import com.rbamzai.jobqueue.worker.handler.JobHandler;
import com.rbamzai.jobqueue.worker.handler.NonRetryableJobException;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Runs one claimed job and records the outcome: succeeded, retry later, or dead-letter. */
@Component
public class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);
    private static final int MAX_ERROR_LENGTH = 2000;

    private final HandlerRegistry handlers;
    private final JobStore store;
    private final Backoff backoff;
    private final WorkerMetrics metrics;

    public JobRunner(HandlerRegistry handlers, JobStore store, Backoff backoff, WorkerMetrics metrics) {
        this.handlers = handlers;
        this.store = store;
        this.backoff = backoff;
        this.metrics = metrics;
    }

    public void run(Job job) {
        long startNanos = System.nanoTime();
        Optional<JobHandler> handler = handlers.find(job.type());
        // Job types come from API clients, so only registered types become metric tags. Otherwise every
        // typo'd type would mint a new time series in Prometheus.
        String typeTag = handler.isPresent() ? job.type() : "unknown";
        Outcome outcome = execute(job, handler);
        metrics.recordExecution(typeTag, outcome, System.nanoTime() - startNanos);
    }

    private Outcome execute(Job job, Optional<JobHandler> handler) {
        try {
            handler.orElseThrow(() -> new NonRetryableJobException("no handler registered for type '" + job.type() + "'"))
                    .handle(job);
        } catch (Exception e) {
            return onFailure(job, e);
        }
        if (store.markSucceeded(job)) {
            log.debug("Job {} ({}) succeeded on attempt {}", job.id(), job.type(), job.attempts());
            return Outcome.SUCCEEDED;
        }
        return lostLease(job, "success");
    }

    private Outcome onFailure(Job job, Exception e) {
        String error = describe(e);
        boolean outOfAttempts = job.attempts() >= job.maxAttempts();

        if (e instanceof NonRetryableJobException || outOfAttempts) {
            if (!store.markDead(job, error)) {
                return lostLease(job, "failure");
            }
            log.warn("Job {} ({}) dead after attempt {}/{}: {}",
                    job.id(), job.type(), job.attempts(), job.maxAttempts(), error);
            return Outcome.DEAD;
        }
        Duration delay = backoff.delayFor(job.attempts());
        if (!store.scheduleRetry(job, delay, error)) {
            return lostLease(job, "failure");
        }
        log.info("Job {} ({}) failed attempt {}/{}, retrying in {} ms: {}",
                job.id(), job.type(), job.attempts(), job.maxAttempts(), delay.toMillis(), error);
        return Outcome.RETRIED;
    }

    private static Outcome lostLease(Job job, String outcome) {
        log.warn("Discarded {} of job {}: its lease was reaped or released, so another worker owns it now",
                outcome, job.id());
        return Outcome.LEASE_LOST;
    }

    private static String describe(Exception e) {
        String text = e.getClass().getSimpleName() + ": " + e.getMessage();
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
