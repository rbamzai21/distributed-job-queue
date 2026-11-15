package com.rbamzai.jobqueue.common;

/**
 * Told about every job that becomes PENDING in Postgres (enqueued, retried, reaped, released, rerun
 * from the DLQ), so a fast-path index can hand it to a worker without polling.
 *
 * <p>Always called after the Postgres write commits, and best-effort: implementations never throw.
 * If a notification is lost, the job is still PENDING in Postgres and the workers' periodic Postgres
 * sweep finds it, so correctness never depends on this.
 */
@FunctionalInterface
public interface ReadyQueue {

    /** Postgres-only mode: nothing to notify, workers poll. */
    ReadyQueue NONE = job -> {};

    /** @param job the row as returned by the write that made it PENDING */
    void offer(Job job);
}
