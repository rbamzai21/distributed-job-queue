package com.rbamzai.jobqueue.common;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with jitter: {@code delay = min(max, base * 2^(attempt - 1)) + random(0, base)}.
 *
 * <p>The exponential part backs off from a struggling dependency; the jitter stops jobs that failed
 * together (say, during an outage) from all retrying in the same instant.
 */
public final class Backoff {

    private final long baseMillis;
    private final long maxMillis;
    private final RandomGenerator random;

    public Backoff(Duration base, Duration max, RandomGenerator random) {
        if (base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("base must be positive, was " + base);
        }
        if (max.compareTo(base) < 0) {
            throw new IllegalArgumentException("max (" + max + ") must be at least base (" + base + ")");
        }
        this.baseMillis = base.toMillis();
        this.maxMillis = max.toMillis();
        this.random = random;
    }

    /**
     * Delay before the next run of a job that has just failed its {@code attempt}-th attempt (1-based).
     * The cap applies before jitter, so a delay can exceed {@code max} by up to {@code base}.
     */
    public Duration delayFor(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
        }
        int exponent = Math.min(attempt - 1, 62);
        // base * 2^exponent > max  <=>  base > max >> exponent; checking this way never overflows.
        long exponential = baseMillis > (maxMillis >> exponent) ? maxMillis : baseMillis << exponent;
        long jitter = random.nextLong(baseMillis + 1);
        return Duration.ofMillis(exponential + jitter);
    }
}
