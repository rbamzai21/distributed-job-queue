package com.rbamzai.jobqueue.worker.handler;

import com.rbamzai.jobqueue.common.Job;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/** Fails at random, for exercising retries and the dead-letter queue. Payload: {"failureRate": 0.5}. */
@Component
public class FlakyHandler implements JobHandler {

    private static final double DEFAULT_FAILURE_RATE = 0.5;

    @Override
    public String type() {
        return "flaky";
    }

    @Override
    public void handle(Job job) {
        double failureRate = job.payload().path("failureRate").asDouble(DEFAULT_FAILURE_RATE);
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new IllegalStateException("simulated failure (failureRate=" + failureRate + ")");
        }
    }
}
