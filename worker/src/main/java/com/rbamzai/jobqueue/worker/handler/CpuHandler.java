package com.rbamzai.jobqueue.worker.handler;

import com.rbamzai.jobqueue.common.Job;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** A CPU-bound job: repeatedly hashes a value. Payload: {"iterations": 100000} (optional). */
@Component
public class CpuHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(CpuHandler.class);
    private static final int DEFAULT_ITERATIONS = 100_000;
    private static final int MAX_ITERATIONS = 10_000_000;

    @Override
    public String type() {
        return "cpu";
    }

    @Override
    public void handle(Job job) throws NoSuchAlgorithmException {
        int iterations = job.payload().path("iterations").asInt(DEFAULT_ITERATIONS);
        if (iterations < 1 || iterations > MAX_ITERATIONS) {
            throw new NonRetryableJobException("payload.iterations must be between 1 and " + MAX_ITERATIONS);
        }
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] digest = job.id().toString().getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < iterations; i++) {
            digest = sha256.digest(digest);
        }
        log.debug("Job {} hashed {} times: {}", job.id(), iterations, HexFormat.of().formatHex(digest));
    }
}
