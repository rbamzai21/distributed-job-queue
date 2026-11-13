package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.Backoff;
import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.worker.handler.HandlerRegistry;
import com.rbamzai.jobqueue.worker.handler.JobHandler;
import com.rbamzai.jobqueue.worker.handler.NonRetryableJobException;
import java.time.Duration;
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

    public JobRunner(HandlerRegistry handlers, JobStore store, Backoff backoff) {
        this.handlers = handlers;
        this.store = store;
        this.backoff = backoff;
    }

    public void run(Job job) {
        try {
            JobHandler handler = handlers.find(job.type())
                    .orElseThrow(() -> new NonRetryableJobException("no handler registered for type '" + job.type() + "'"));
            handler.handle(job);
        } catch (Exception e) {
            onFailure(job, e);
            return;
        }
        if (store.markSucceeded(job)) {
            log.debug("Job {} ({}) succeeded on attempt {}", job.id(), job.type(), job.attempts());
        } else {
            logLostLease(job, "success");
        }
    }

    private void onFailure(Job job, Exception e) {
        String error = describe(e);
        boolean outOfAttempts = job.attempts() >= job.maxAttempts();

        if (e instanceof NonRetryableJobException || outOfAttempts) {
            if (store.markDead(job, error)) {
                log.warn("Job {} ({}) dead after attempt {}/{}: {}",
                        job.id(), job.type(), job.attempts(), job.maxAttempts(), error);
            } else {
                logLostLease(job, "failure");
            }
        } else {
            Duration delay = backoff.delayFor(job.attempts());
            if (store.scheduleRetry(job, delay, error)) {
                log.info("Job {} ({}) failed attempt {}/{}, retrying in {} ms: {}",
                        job.id(), job.type(), job.attempts(), job.maxAttempts(), delay.toMillis(), error);
            } else {
                logLostLease(job, "failure");
            }
        }
    }

    private static void logLostLease(Job job, String outcome) {
        log.warn("Discarded {} of job {}: its lease was reaped or released, so another worker owns it now",
                outcome, job.id());
    }

    private static String describe(Exception e) {
        String text = e.getClass().getSimpleName() + ": " + e.getMessage();
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
