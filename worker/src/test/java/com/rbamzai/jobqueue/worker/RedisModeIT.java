package com.rbamzai.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobStatus;
import com.rbamzai.jobqueue.common.RedisReadyQueue;
import com.rbamzai.jobqueue.worker.handler.JobHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Workers on the Redis fast path, including the failure modes that make Redis safe to use as a mere
 * index: lost notifications, lost Redis data, and Redis going down entirely.
 */
class RedisModeIT extends RedisTestSupport {

    private final List<TestWorker> workers = new ArrayList<>();
    private final Map<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();
    /** Plays the API's part: offers each enqueued job to Redis after inserting it. */
    private RedisReadyQueue api;

    @BeforeEach
    void setUp() {
        api = new RedisReadyQueue(redisUrl());
    }

    @AfterEach
    void tearDown() {
        workers.forEach(TestWorker::close);
        api.close();
    }

    @Test
    void manyWorkersRunEveryJobExactlyOnce() {
        for (int i = 0; i < 4; i++) {
            startWorker(TestWorker.fastRedisProperties("w" + i, 8, redisUrl()));
        }

        for (int i = 0; i < 1000; i++) {
            api.offer(insertJob("count", 0, 5, 0));
        }

        awaitSucceeded(1000, Duration.ofSeconds(60));
        assertThat(executions).hasSize(1000);
        assertThat(executions.values()).allMatch(count -> count.get() == 1);
        assertThat(allJobs()).allMatch(job -> job.attempts() == 1);
    }

    /** The API committed the job but crashed before the ZADD: the Postgres sweep still finds it. */
    @Test
    void jobsMissingFromRedisAreFoundByThePostgresSweep() {
        startWorker(TestWorker.fastRedisProperties("w", 8, redisUrl()));

        for (int i = 0; i < 20; i++) {
            insertJob("count", 0, 5, 0); // no offer
        }

        awaitSucceeded(20, Duration.ofSeconds(10));
    }

    /** Redis restarts empty mid-run: nothing is lost, because Postgres is the source of truth. */
    @Test
    void losingAllRedisDataLosesNoJobs() {
        for (int i = 0; i < 200; i++) {
            api.offer(insertJob("count", 0, 5, 0));
        }
        flushRedis();
        startWorker(TestWorker.fastRedisProperties("w", 8, redisUrl()));

        awaitSucceeded(200, Duration.ofSeconds(30));
    }

    /** Retries go through the delayed set and come back via the promoter. */
    @Test
    void failedJobsAreRetriedThroughTheDelayedSet() {
        startWorker(TestWorker.fastRedisProperties("w", 8, redisUrl()));

        Job flaky = insertJob("fail-twice", 0, 5, 0);
        api.offer(flaky);

        await().atMost(Duration.ofSeconds(10)).until(() -> job(flaky.id()).status() == JobStatus.SUCCEEDED);
        assertThat(job(flaky.id()).attempts()).isEqualTo(3);
    }

    /** Redis dies mid-run: workers degrade to Postgres polling and every job still completes. */
    @Test
    void workersFallBackToPostgresWhenRedisGoesDown() {
        try (GenericContainer<?> doomedRedis = new GenericContainer<>("redis:7.4").withExposedPorts(6379)) {
            doomedRedis.start();
            String url = redisUrl(doomedRedis);
            startWorker(TestWorker.fastRedisProperties("w1", 8, url));
            startWorker(TestWorker.fastRedisProperties("w2", 8, url));
            try (RedisReadyQueue doomedApi = new RedisReadyQueue(url)) {
                for (int i = 0; i < 100; i++) {
                    doomedApi.offer(insertJob("count", 0, 5, 0));
                }
                awaitSucceeded(100, Duration.ofSeconds(10));

                doomedRedis.stop();

                for (int i = 0; i < 100; i++) {
                    doomedApi.offer(insertJob("count", 0, 5, 0)); // fails quietly: Redis is gone
                }
            }
            awaitSucceeded(200, Duration.ofSeconds(30));
        }
    }

    private void startWorker(WorkerProperties properties) {
        workers.add(new TestWorker(properties, List.of(countingHandler(), failTwiceHandler())).start());
    }

    private void awaitSucceeded(long count, Duration timeout) {
        await().atMost(timeout).until(() -> countByStatus().getOrDefault(JobStatus.SUCCEEDED, 0L) == count);
    }

    private JobHandler countingHandler() {
        return new JobHandler() {
            @Override
            public String type() {
                return "count";
            }

            @Override
            public void handle(Job job) throws InterruptedException {
                Thread.sleep(2);
                executions.computeIfAbsent(job.id(), id -> new AtomicInteger()).incrementAndGet();
            }
        };
    }

    private static JobHandler failTwiceHandler() {
        return new JobHandler() {
            @Override
            public String type() {
                return "fail-twice";
            }

            @Override
            public void handle(Job job) {
                if (job.attempts() <= 2) {
                    throw new IllegalStateException("attempt " + job.attempts());
                }
            }
        };
    }
}
