package com.rbamzai.jobqueue.common;

/** How workers find runnable jobs. Postgres is the source of truth in both modes. */
public enum QueueMode {
    /** Workers poll Postgres with SELECT ... FOR UPDATE SKIP LOCKED. */
    POSTGRES,
    /** Workers block on a Redis sorted set of ready job ids, then claim those rows in Postgres. */
    REDIS
}
