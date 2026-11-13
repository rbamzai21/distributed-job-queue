package com.rbamzai.jobqueue.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class BackoffTest {

    private static final RandomGenerator NO_JITTER = () -> 0L;

    @Test
    void firstRetryWaitsBaseDelay() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofMinutes(5), NO_JITTER);

        assertThat(backoff.delayFor(1)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void delayDoublesEachAttempt() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofMinutes(5), NO_JITTER);

        assertThat(backoff.delayFor(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(backoff.delayFor(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(backoff.delayFor(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(backoff.delayFor(5)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    void delayIsCappedAtMax() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), NO_JITTER);

        assertThat(backoff.delayFor(7)).isEqualTo(Duration.ofSeconds(60)); // 64s uncapped
        assertThat(backoff.delayFor(20)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void hugeAttemptCountDoesNotOverflow() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofMinutes(5), NO_JITTER);

        assertThat(backoff.delayFor(Integer.MAX_VALUE)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void jitterStaysWithinZeroToBase() {
        Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofMinutes(5), new SplittableRandom(42));
        Set<Duration> seen = new HashSet<>();

        for (int i = 0; i < 10_000; i++) {
            Duration delay = backoff.delayFor(3); // 400ms before jitter
            assertThat(delay).isBetween(Duration.ofMillis(400), Duration.ofMillis(500));
            seen.add(delay);
        }

        assertThat(seen).as("jitter should actually vary the delay").hasSizeGreaterThan(50);
    }

    @Test
    void rejectsAttemptsBelowOne() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofMinutes(5), NO_JITTER);

        assertThatThrownBy(() -> backoff.delayFor(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMaxBelowBase() {
        assertThatThrownBy(() -> new Backoff(Duration.ofSeconds(10), Duration.ofSeconds(1), NO_JITTER))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
