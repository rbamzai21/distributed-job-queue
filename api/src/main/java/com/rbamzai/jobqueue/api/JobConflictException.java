package com.rbamzai.jobqueue.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/** The request is valid but conflicts with the job's current state. */
@ResponseStatus(HttpStatus.CONFLICT)
public class JobConflictException extends RuntimeException {

    public JobConflictException(String message) {
        super(message);
    }
}
