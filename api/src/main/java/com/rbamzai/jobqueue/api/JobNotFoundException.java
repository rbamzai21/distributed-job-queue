package com.rbamzai.jobqueue.api;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(UUID id) {
        super("job " + id + " not found");
    }
}
