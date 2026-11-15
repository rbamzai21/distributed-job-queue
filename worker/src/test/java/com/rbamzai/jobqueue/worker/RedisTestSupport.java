package com.rbamzai.jobqueue.worker;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;

/** Adds one shared Redis to the Postgres test setup. Each test starts with both empty. */
abstract class RedisTestSupport extends PostgresTestSupport {

    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4").withExposedPorts(6379);

    static {
        REDIS.start();
    }

    static String redisUrl() {
        return redisUrl(REDIS);
    }

    static String redisUrl(GenericContainer<?> redis) {
        return "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    }

    @BeforeEach
    void flushRedis() {
        RedisClient client = RedisClient.create(redisUrl());
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            connection.sync().flushall();
        } finally {
            client.shutdown();
        }
    }
}
