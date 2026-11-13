package com.rbamzai.jobqueue.worker.handler;

/** A failure that retrying won't fix, such as a malformed payload. The job goes straight to DEAD. */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }
}
