package com.rbamzai.jobqueue.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rbamzai.jobqueue.common.Backoff;
import com.rbamzai.jobqueue.common.QueueMode;
import com.rbamzai.jobqueue.common.ReadyQueue;
import com.rbamzai.jobqueue.common.RedisReadyQueue;
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
 * A complete worker (claim loop, heartbeats, reaper, and in Redis mode the promoter) wired by hand
 * instead of by Spring, so a test can run several in one JVM, each with its own connection pool, and
 * crash them individually.
 */
final class TestWorker implements AutoCloseable {

    /** Short timings so crash recovery happens in seconds rather than the production 30s lease. */
    static WorkerProperties fastProperties(String id, int concurrency, Duration shutdownTimeout) {
        return properties(id, QueueMode.POSTGRES, concurrency, shutdownTimeout, null);
    }

    static WorkerProperties fastRedisProperties(String id, int concurrency, String redisUrl) {
        return properties(id, QueueMode.REDIS, concurrency, Duration.ofSeconds(1), redisUrl);
    }

    private static WorkerProperties properties(
            String id, QueueMode mode, int concurrency, Duration shutdownTimeout, String redisUrl) {
        return new WorkerProperties(
                id,
                mode,
                concurrency,
                10,
                Duration.ofMillis(50),   // poll interval / BZPOPMIN timeout
                Duration.ofSeconds(2),   // lease
                Duration.ofMillis(500),  // heartbeat
                Duration.ofMillis(300),  // reaper
                Duration.ofMillis(100),  // backoff base
                Duration.ofSeconds(1),   // backoff max
                shutdownTimeout,
                redisUrl,
                Duration.ofSeconds(1),   // Redis mode: Postgres sweep
                Duration.ofMillis(50));  // Redis mode: delayed-job promotion
    }

    final String id;
    private final HikariDataSource dataSource;
    private final ReadyQueue readyQueue;
    private final ClaimLoop loop;
    private final LeaseReaper reaper;
    private final DelayedJobPromoter promoter;
    private final WorkerProperties properties;
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2);

    TestWorker(WorkerProperties properties, List<JobHandler> handlers) {
        this.id = properties.id();
        this.properties = properties;
        this.dataSource = PostgresTestSupport.newDataSource();
        WorkerMetrics metrics = new WorkerMetrics(new SimpleMeterRegistry());
        WorkerIdentity identity = new WorkerIdentity(id);
        JobStore store = new JobStore(JdbcClient.create(dataSource), PostgresTestSupport.MAPPER);

        ClaimSource source;
        if (properties.queueMode() == QueueMode.REDIS) {
            RedisReadyQueue redis = new RedisReadyQueue(properties.redisUrl());
            this.readyQueue = redis;
            this.promoter = new DelayedJobPromoter(redis);
            source = new RedisClaimSource(store, redis, properties, metrics, identity);
        } else {
            this.readyQueue = ReadyQueue.NONE;
            this.promoter = null;
            source = new PostgresClaimSource(store, properties, metrics, identity);
        }

        Backoff backoff = new Backoff(properties.backoffBase(), properties.backoffMax(), new Random());
        JobRunner runner = new JobRunner(new HandlerRegistry(handlers), store, backoff, readyQueue, metrics);
        LeaseKeeper leases = new LeaseKeeper(store, readyQueue, properties, metrics);
        this.loop = new ClaimLoop(source, runner, leases, properties, identity);
        this.reaper = new LeaseReaper(store, readyQueue, metrics);
    }

    TestWorker start() {
        loop.start();
        schedule(reaper::reap, properties.reaperInterval());
        if (promoter != null) {
            schedule(promoter::promote, properties.promoteInterval());
        }
        return this;
    }

    /** Graceful shutdown, exactly as on SIGTERM. */
    void stop() {
        loop.stop();
        timers.shutdownNow();
    }

    /**
     * Simulates the process dying or losing the network: every database connection is cut, so it can't
     * heartbeat, finish jobs, or release leases. Its handler threads keep running, like a worker that is
     * partitioned rather than dead, which is the harder case for correctness.
     */
    void crash() {
        timers.shutdownNow();
        dataSource.close();
    }

    @Override
    public void close() {
        if (loop.isRunning()) {
            loop.stop();
        }
        timers.shutdownNow();
        dataSource.close();
        if (readyQueue instanceof RedisReadyQueue redis) {
            redis.close();
        }
    }

    private void schedule(Runnable task, Duration interval) {
        long millis = interval.toMillis();
        timers.scheduleWithFixedDelay(task, millis, millis, MILLISECONDS);
    }
}
