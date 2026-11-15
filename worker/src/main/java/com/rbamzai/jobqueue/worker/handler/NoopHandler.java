package com.rbamzai.jobqueue.worker.handler;

import com.rbamzai.jobqueue.common.Job;
import org.springframework.stereotype.Component;

/** Does nothing. Benchmarks use it so they measure the queue itself, not the work. */
@Component
public class NoopHandler implements JobHandler {

    @Override
    public String type() {
        return "noop";
    }

    @Override
    public void handle(Job job) {}
}
