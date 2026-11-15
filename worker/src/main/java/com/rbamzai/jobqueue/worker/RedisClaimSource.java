package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.RedisReadyQueue;
import io.lettuce.core.RedisException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The fast path: block on the Redis ready set until a job id arrives, then claim exactly those rows in
 * Postgres. An idle worker wakes the moment a job is enqueued instead of on its next poll, and never
 * scans Postgres for work.
 *
 * <p>Redis is only an index, so two safety nets cover everything it can miss: a job whose ZADD was lost
 * (API crashed after committing), ids popped by a worker that then died, or a Redis restart that lost
 * the sets.
 * <ol>
 *   <li>Every {@code redisSweepInterval}, the worker also runs the normal Postgres SKIP LOCKED claim.</li>
 *   <li>While Redis is unreachable, the worker degrades to plain Postgres polling and recovers on its own.</li>
 * </ol>
 */
public class RedisClaimSource implements ClaimSource {

    private static final Logger log = LoggerFactory.getLogger(RedisClaimSource.class);
    private static final Duration DRAINING_WAIT = Duration.ofMillis(1);

    private final JobStore store;
    private final RedisReadyQueue readyQueue;
    private final WorkerProperties properties;
    private final WorkerMetrics metrics;
    private final String workerId;

    private long lastSweepNanos = System.nanoTime();
    private boolean draining;
    private boolean redisDown;

    public RedisClaimSource(
            JobStore store,
            RedisReadyQueue readyQueue,
            WorkerProperties properties,
            WorkerMetrics metrics,
            WorkerIdentity identity) {
        this.store = store;
        this.readyQueue = readyQueue;
        this.properties = properties;
        this.metrics = metrics;
        this.workerId = identity.id();
    }

    @Override
    public List<Job> claim(int max) throws InterruptedException {
        if (System.nanoTime() - lastSweepNanos >= properties.redisSweepInterval().toNanos()) {
            lastSweepNanos = System.nanoTime();
            List<Job> swept = sweep(max);
            if (!swept.isEmpty()) {
                return swept;
            }
        }

        List<UUID> ids;
        try {
            // While draining, Redis is known to be missing jobs Postgres has, so don't block on it.
            ids = readyQueue.popReady(max, draining ? DRAINING_WAIT : properties.pollInterval());
        } catch (RedisException e) {
            if (!redisDown) {
                redisDown = true;
                log.warn("Redis unavailable, falling back to Postgres polling: {}", e.getMessage());
            }
            List<Job> polled = claimFromPostgres(max, "postgres");
            if (polled.isEmpty()) {
                Thread.sleep(properties.pollInterval().toMillis());
            }
            return polled;
        }
        if (redisDown) {
            redisDown = false;
            log.info("Redis is back; resuming the Redis fast path");
        }
        if (ids.isEmpty()) {
            return draining ? sweep(max) : List.of();
        }

        long startNanos = System.nanoTime();
        List<Job> claimed = store.claimByIds(workerId, ids, properties.leaseDuration());
        metrics.recordClaim("redis", claimed, System.nanoTime() - startNanos);
        return claimed;
    }

    @Override
    public boolean waitsForWork() {
        return true;
    }

    /**
     * Claims from Postgres directly. If that finds work, Redis may be missing more (say it restarted
     * empty), so keep draining Postgres whenever Redis comes up empty until Postgres is empty too,
     * instead of recovering only one batch per sweep interval.
     */
    private List<Job> sweep(int max) {
        List<Job> swept = claimFromPostgres(max, "sweep");
        draining = !swept.isEmpty();
        return swept;
    }

    private List<Job> claimFromPostgres(int max, String source) {
        long startNanos = System.nanoTime();
        List<Job> claimed = store.claim(workerId, max, properties.leaseDuration());
        metrics.recordClaim(source, claimed, System.nanoTime() - startNanos);
        return claimed;
    }
}
