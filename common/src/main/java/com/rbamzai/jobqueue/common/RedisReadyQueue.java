package com.rbamzai.jobqueue.common;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Redis index of runnable job ids, so workers can block until work arrives instead of polling Postgres.
 *
 * <p>Two sorted sets:
 * <ul>
 *   <li>{@code jobqueue:ready}: score {@code -priority}, member {@code <run_at ms, zero-padded>:<id>}.
 *       BZPOPMIN pops the lowest score (highest priority), and Redis orders equal scores by member, which
 *       the zero-padding makes chronological: highest priority first, then oldest first, matching the
 *       Postgres claim order exactly.</li>
 *   <li>{@code jobqueue:delayed}: score {@code run_at} in ms, member {@code <priority>:<id>}. A Lua script
 *       moves due members to the ready set atomically.</li>
 * </ul>
 *
 * <p>Entries can go stale (a job is cancelled, or claimed through the Postgres sweep). That is harmless:
 * the Postgres claim only takes rows that are still PENDING and due, and silently drops the rest.
 */
public final class RedisReadyQueue implements ReadyQueue, AutoCloseable {

    public static final String READY_KEY = "jobqueue:ready";
    public static final String DELAYED_KEY = "jobqueue:delayed";

    private static final Logger log = LoggerFactory.getLogger(RedisReadyQueue.class);

    /** Uses Redis's own clock, so every worker promotes against the same time. */
    private static final String PROMOTE_DUE = """
            local now = redis.call('TIME')
            local nowMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
            local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', nowMs, 'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[1]))
            for i = 1, #due, 2 do
              local member = due[i]
              local sep = string.find(member, ':', 1, true)
              local priority = tonumber(string.sub(member, 1, sep - 1))
              local id = string.sub(member, sep + 1)
              redis.call('ZREM', KEYS[1], member)
              redis.call('ZADD', KEYS[2], -priority, string.format('%015d', tonumber(due[i + 1])) .. ':' .. id)
            end
            return #due / 2
            """;

    private final RedisClient client;
    /** Shared, thread-safe connection for non-blocking commands. */
    private final StatefulRedisConnection<String, String> connection;
    /** BZPOPMIN blocks its connection, so it gets its own. Only the claim loop thread uses it. */
    private final StatefulRedisConnection<String, String> blockingConnection;

    public RedisReadyQueue(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
        client.setOptions(ClientOptions.builder()
                // Fail fast while Redis is down, so callers fall back to Postgres instead of queueing up.
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(10)))
                .build());
        this.connection = client.connect();
        this.blockingConnection = client.connect();
    }

    @Override
    public void offer(Job job) {
        try {
            if (job.runAt().isAfter(job.updatedAt())) {
                connection.sync().zadd(DELAYED_KEY, job.runAt().toEpochMilli(), job.priority() + ":" + job.id());
            } else {
                connection.sync().zadd(READY_KEY, -job.priority(), readyMember(job));
            }
        } catch (RedisException e) {
            log.warn("Could not index job {} in Redis; the Postgres sweep will pick it up: {}", job.id(), e.getMessage());
        }
    }

    /**
     * Waits up to {@code wait} for a ready job, then takes up to {@code max} ids, best first.
     *
     * @throws RedisException if Redis is unreachable, so the caller can fall back to Postgres
     */
    public List<UUID> popReady(int max, Duration wait) {
        KeyValue<String, ScoredValue<String>> first =
                blockingConnection.sync().bzpopmin(wait.toMillis() / 1000.0, READY_KEY);
        if (first == null || !first.hasValue()) {
            return List.of();
        }
        List<UUID> ids = new ArrayList<>(max);
        ids.add(idOf(first.getValue().getValue()));
        if (max > 1) {
            for (ScoredValue<String> next : connection.sync().zpopmin(READY_KEY, max - 1)) {
                ids.add(idOf(next.getValue()));
            }
        }
        return ids;
    }

    /** Moves up to {@code limit} due delayed jobs to the ready set. Returns how many moved. */
    public long promoteDue(int limit) {
        Long moved = connection.sync().eval(
                PROMOTE_DUE, ScriptOutputType.INTEGER, new String[] {DELAYED_KEY, READY_KEY}, String.valueOf(limit));
        return moved == null ? 0 : moved;
    }

    public long readySize() {
        return connection.sync().zcard(READY_KEY);
    }

    @Override
    public void close() {
        blockingConnection.close();
        connection.close();
        client.shutdown();
    }

    static String readyMember(Job job) {
        return String.format("%015d:%s", job.runAt().toEpochMilli(), job.id());
    }

    private static UUID idOf(String member) {
        return UUID.fromString(member.substring(member.indexOf(':') + 1));
    }
}
