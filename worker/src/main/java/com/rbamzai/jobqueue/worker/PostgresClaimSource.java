package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.Job;
import java.util.List;

/** Polls Postgres with FOR UPDATE SKIP LOCKED. When the queue is empty, the loop sleeps one poll interval. */
public class PostgresClaimSource implements ClaimSource {

    private final JobStore store;
    private final WorkerProperties properties;
    private final WorkerMetrics metrics;
    private final String workerId;

    public PostgresClaimSource(JobStore store, WorkerProperties properties, WorkerMetrics metrics, WorkerIdentity identity) {
        this.store = store;
        this.properties = properties;
        this.metrics = metrics;
        this.workerId = identity.id();
    }

    @Override
    public List<Job> claim(int max) {
        long startNanos = System.nanoTime();
        List<Job> claimed = store.claim(workerId, max, properties.leaseDuration());
        metrics.recordClaim("postgres", claimed, System.nanoTime() - startNanos);
        return claimed;
    }

    @Override
    public boolean waitsForWork() {
        return false;
    }
}
