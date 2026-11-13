package com.rbamzai.jobqueue.worker.handler;

import com.rbamzai.jobqueue.common.Job;

/**
 * Runs jobs of one type. Register a handler by making it a Spring bean.
 *
 * <p>Delivery is at-least-once: if a worker crashes after {@code handle} finishes but before the job
 * is marked done, the job runs again. Handlers must therefore be idempotent, meaning that running a
 * job twice has the same effect as running it once (for example, by using the job id as a dedup key
 * with whatever external system the handler calls).
 *
 * <p>Throw {@link NonRetryableJobException} for failures a retry can't fix; any other exception is
 * retried with backoff until the job runs out of attempts.
 */
public interface JobHandler {

    String type();

    void handle(Job job) throws Exception;
}
