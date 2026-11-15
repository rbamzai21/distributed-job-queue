package com.rbamzai.jobqueue.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rbamzai.jobqueue.common.Backoff;
import com.rbamzai.jobqueue.worker.handler.HandlerRegistry;
import com.rbamzai.jobqueue.worker.handler.JobHandler;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * A complete worker (claim loop, heartbeats, reaper) wired by hand instead of by Spring, so a test can
 * run several in one JVM, each with its own connection pool, and crash them individually.
 */
final class TestWorker implements AutoCloseable {

    /** Short timings so crash recovery happens in seconds rather than the production 30s lease. */
    static WorkerProperties fastProperties(String id, int concurrency, Duration shutdownTimeout) {
        return new WorkerProperties(
                id,
                concurrency,
                10,
                Duration.ofMillis(50),   // poll interval
                Duration.ofSeconds(2),   // lease
                Duration.ofMillis(500),  // heartbeat
                Duration.ofMillis(300),  // reaper
                Duration.ofMillis(100),  // backoff base
                Duration.ofSeconds(1),   // backoff max
                shutdownTimeout);
    }

    final String id;
    private final HikariDataSource dataSource;
    private final ClaimLoop loop;
    private final LeaseReaper reaper;
    private final Duration reaperInterval;
    private final ScheduledExecutorService reaperTimer = Executors.newSingleThreadScheduledExecutor();

    TestWorker(WorkerProperties properties, List<JobHandler> handlers) {
        this.id = properties.id();
        this.dataSource = PostgresTestSupport.newDataSource();
        WorkerMetrics metrics = new WorkerMetrics(new SimpleMeterRegistry());
        JobStore store = new JobStore(JdbcClient.create(dataSource), PostgresTestSupport.MAPPER);
        Backoff backoff = new Backoff(properties.backoffBase(), properties.backoffMax(), new Random());
        JobRunner runner = new JobRunner(new HandlerRegistry(handlers), store, backoff, metrics);
        LeaseKeeper leases = new LeaseKeeper(store, properties, metrics);
        this.loop = new ClaimLoop(store, runner, leases, properties, new WorkerIdentity(id), metrics);
        this.reaper = new LeaseReaper(store, metrics);
        this.reaperInterval = properties.reaperInterval();
    }

    TestWorker start() {
        loop.start();
        long interval = reaperInterval.toMillis();
        reaperTimer.scheduleWithFixedDelay(reaper::reap, interval, interval, MILLISECONDS);
        return this;
    }

    /** Graceful shutdown, exactly as on SIGTERM. */
    void stop() {
        loop.stop();
        reaperTimer.shutdownNow();
    }

    /**
     * Simulates the process dying or losing the network: every database connection is cut, so it can't
     * heartbeat, finish jobs, or release leases. Its handler threads keep running, like a worker that is
     * partitioned rather than dead, which is the harder case for correctness.
     */
    void crash() {
        reaperTimer.shutdownNow();
        dataSource.close();
    }

    @Override
    public void close() {
        if (loop.isRunning()) {
            loop.stop();
        }
        reaperTimer.shutdownNow();
        dataSource.close();
    }
}
