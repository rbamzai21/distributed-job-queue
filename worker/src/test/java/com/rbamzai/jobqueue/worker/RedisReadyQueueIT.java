package com.rbamzai.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.RedisReadyQueue;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The Redis index on its own: ordering, blocking, and delayed-job promotion. */
class RedisReadyQueueIT extends RedisTestSupport {

    private RedisReadyQueue queue;

    @BeforeEach
    void setUp() {
        queue = new RedisReadyQueue(redisUrl());
    }

    @AfterEach
    void tearDown() {
        queue.close();
    }

    /** Must match the Postgres claim order exactly: priority DESC, then run_at ASC. */
    @Test
    void popsHighestPriorityFirstThenOldestFirst() throws InterruptedException {
        Job oldLow = insertJob("t", 0, 5, 0);
        Thread.sleep(5); // distinct run_at milliseconds, so the FIFO tiebreak is what's being tested
        Job newLow = insertJob("t", 0, 5, 0);
        Job high = insertJob("t", 10, 5, 0);
        Job negative = insertJob("t", -5, 5, 0);
        // Offer out of order, to show the order comes from the scores, not insertion.
        List.of(negative, newLow, high, oldLow).forEach(queue::offer);

        List<UUID> popped = queue.popReady(10, Duration.ofMillis(100));

        assertThat(popped).containsExactly(high.id(), oldLow.id(), newLow.id(), negative.id());
    }

    @Test
    void popBlocksUntilTheTimeoutWhenEmpty() {
        long start = System.nanoTime();

        assertThat(queue.popReady(5, Duration.ofMillis(300))).isEmpty();

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(250));
    }

    /** The point of the fast path: a blocked worker wakes as soon as a job is offered, not on its next poll. */
    @Test
    void blockedPopWakesAsSoonAsAJobIsOffered() throws Exception {
        Job job = insertJob("t", 0, 5, 0);
        Thread offerer = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                return;
            }
            queue.offer(job);
        });

        long start = System.nanoTime();
        List<UUID> popped = queue.popReady(1, Duration.ofSeconds(5));
        Duration waited = Duration.ofNanos(System.nanoTime() - start);
        offerer.join();

        assertThat(popped).containsExactly(job.id());
        assertThat(waited).isBetween(Duration.ofMillis(150), Duration.ofSeconds(1));
    }

    @Test
    void delayedJobsOnlyBecomeReadyOnceDue() throws InterruptedException {
        Job delayed = insertJob("t", 3, 5, 0.5);
        queue.offer(delayed);

        queue.promoteDue(100);
        assertThat(queue.popReady(1, Duration.ofMillis(50))).isEmpty();

        Thread.sleep(700);
        assertThat(queue.promoteDue(100)).isEqualTo(1);
        assertThat(queue.popReady(1, Duration.ofMillis(50))).containsExactly(delayed.id());
    }

    @Test
    void offeringTheSameJobTwiceIndexesItOnce() {
        Job job = insertJob("t", 0, 5, 0);

        queue.offer(job);
        queue.offer(job);

        assertThat(queue.readySize()).isEqualTo(1);
    }
}
