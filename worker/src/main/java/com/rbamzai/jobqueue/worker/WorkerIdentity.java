package com.rbamzai.jobqueue.worker;

/** This worker process's name, recorded in jobs.worker_id for debugging. Ownership is enforced by lease_id. */
public record WorkerIdentity(String id) {}
