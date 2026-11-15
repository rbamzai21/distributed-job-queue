package com.rbamzai.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobStatus;
import com.rbamzai.jobqueue.worker.handler.JobHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** End-to-end behavior of several workers sharing one queue, including crashes and shutdowns. */
class QueueReliabilityIT extends PostgresTestSupport {

    private final List<TestWorker> workers = new ArrayList<>();

    @AfterEach
    void stopWorkers() {
        workers.forEach(TestWorker::close);
    }

    /** N workers, M jobs: every job succeeds, and no job is ever handed to two workers. */
    @Test
    void manyWorkersRunEveryJobExactlyOnce() {
        int jobCount = 1000;
        CountingHandler handler = new CountingHandler("count", Duration.ofMillis(5));
        for (int i = 0; i < 4; i++) {
            startWorker(TestWorker.fastProperties("w" + i, 8, Duration.ofSeconds(5)), handler);
        }

        for (int i = 0; i < jobCount; i++) {
            enqueue("count");
        }

        await().atMost(Duration.ofSeconds(60))
                .until(() -> countByStatus().getOrDefault(JobStatus.SUCCEEDED, 0L) == jobCount);

        assertThat(handler.executions).hasSize(jobCount);
        assertThat(handler.executions.values()).allMatch(count -> count.get() == 1);
        List<Job> jobs = allJobs();
        assertThat(jobs).allMatch(job -> job.attempts() == 1);
        assertThat(jobs.stream().map(Job::workerId).collect(Collectors.toSet()))
                .as("the load should be spread over several workers")
                .hasSizeGreaterThan(1);
    }

    /**
     * Kill a worker mid-job: its leases expire, the other worker's reaper requeues the jobs, and they
     * all complete. The crashed worker's handlers also ran to the end, so each of those jobs executed
     * twice: at-least-once delivery, which is why handlers must be idempotent.
     */
    @Test
    void jobsSurviveAWorkerCrashingMidRun() {
        CountingHandler handler = new CountingHandler("slow", Duration.ofMillis(1500));
        TestWorker doomed = startWorker(TestWorker.fastProperties("doomed", 8, Duration.ofSeconds(1)), handler);
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            ids.add(enqueue("slow"));
        }
        await().atMost(Duration.ofSeconds(5))
                .until(() -> countByStatus().getOrDefault(JobStatus.RUNNING, 0L) == 8);

        doomed.crash();
        startWorker(TestWorker.fastProperties("survivor", 8, Duration.ofSeconds(1)), handler);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> countByStatus().getOrDefault(JobStatus.SUCCEEDED, 0L) == 8);

        for (UUID id : ids) {
            Job job = job(id);
            assertThat(job.workerId()).isEqualTo("survivor");
            assertThat(job.attempts()).isEqualTo(2);
            assertThat(job.lastError()).startsWith("lease expired on worker doomed");
        }
        await().atMost(Duration.ofSeconds(5))
                .until(() -> handler.executions.values().stream().allMatch(count -> count.get() == 2));
    }

    /**
     * SIGTERM with a mix of jobs: short ones finish on the stopping worker; long ones outlast the
     * shutdown timeout, get their leases released, and another worker picks them up right away
     * (well before the stale leases would have expired).
     */
    @Test
    void gracefulShutdownFinishesShortJobsAndReleasesLongOnes() {
        CountingHandler shortHandler = new CountingHandler("short", Duration.ofMillis(300));
        CountingHandler longHandler = new CountingHandler("long", Duration.ofSeconds(30));
        TestWorker stopping = startWorker(
                TestWorker.fastProperties("stopping", 8, Duration.ofSeconds(1)), shortHandler, longHandler);
        List<UUID> shortJobs = List.of(enqueue("short"), enqueue("short"));
        List<UUID> longJobs = List.of(enqueue("long"), enqueue("long"));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> countByStatus().getOrDefault(JobStatus.RUNNING, 0L) == 4);

        stopping.stop();

        for (UUID id : shortJobs) {
            assertThat(job(id).status()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(job(id).workerId()).isEqualTo("stopping");
        }
        for (UUID id : longJobs) {
            Job job = job(id);
            assertThat(job.status()).isEqualTo(JobStatus.PENDING);
            assertThat(job.leaseId()).isNull();
            assertThat(job.lastError()).isEqualTo("released by worker shutdown");
        }

        // The interrupted long handlers try to record a failure after the release; fencing must drop it.
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> longJobs.stream().allMatch(id -> job(id).status() == JobStatus.PENDING));

        startWorker(TestWorker.fastProperties("next", 8, Duration.ofSeconds(1)),
                shortHandler, new CountingHandler("long", Duration.ofMillis(10)));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> longJobs.stream().allMatch(id -> job(id).status() == JobStatus.SUCCEEDED));
        assertThat(longJobs).allMatch(id -> job(id).workerId().equals("next"));
    }

    @Test
    void failingJobsRetryWithBackoffThenDeadLetter() {
        JobHandler alwaysFails = new JobHandler() {
            @Override
            public String type() {
                return "broken";
            }

            @Override
            public void handle(Job job) {
                throw new IllegalStateException("boom");
            }
        };
        startWorker(TestWorker.fastProperties("w", 8, Duration.ofSeconds(1)), alwaysFails);
        UUID id = enqueue("broken", 0, 3, 0);

        await().atMost(Duration.ofSeconds(10)).until(() -> job(id).status() == JobStatus.DEAD);

        Job dead = job(id);
        assertThat(dead.attempts()).isEqualTo(3);
        assertThat(dead.lastError()).isEqualTo("IllegalStateException: boom");
    }

    private TestWorker startWorker(WorkerProperties properties, JobHandler... handlers) {
        TestWorker worker = new TestWorker(properties, List.of(handlers)).start();
        workers.add(worker);
        return worker;
    }

    /** Sleeps to simulate work and counts how many times each job actually executed. */
    private static final class CountingHandler implements JobHandler {

        final Map<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();
        private final String type;
        private final Duration duration;

        CountingHandler(String type, Duration duration) {
            this.type = type;
            this.duration = duration;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public void handle(Job job) throws InterruptedException {
            Thread.sleep(duration.toMillis() + ThreadLocalRandom.current().nextLong(0, 3));
            executions.computeIfAbsent(job.id(), id -> new AtomicInteger()).incrementAndGet();
        }
    }
}
