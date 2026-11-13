package com.rbamzai.jobqueue.common;

public enum JobStatus {
    /** Waiting to be claimed. A job waiting to retry is PENDING with a future run_at. */
    PENDING,
    /** Claimed by a worker that holds its lease. */
    RUNNING,
    SUCCEEDED,
    /** Out of attempts or failed permanently: the dead-letter queue. */
    DEAD,
    CANCELLED
}
