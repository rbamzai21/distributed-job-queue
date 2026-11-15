package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.ReadyQueue;
import com.rbamzai.jobqueue.common.RedisReadyQueue;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wires the claim path for {@code worker.queue-mode}: postgres (default) or redis. */
@Configuration
class QueueModeConfig {

    @Configuration
    @ConditionalOnProperty(name = "worker.queue-mode", havingValue = "postgres", matchIfMissing = true)
    static class PostgresMode {

        @Bean
        ReadyQueue readyQueue() {
            return ReadyQueue.NONE;
        }

        @Bean
        ClaimSource claimSource(
                JobStore store, WorkerProperties properties, WorkerMetrics metrics, WorkerIdentity identity) {
            return new PostgresClaimSource(store, properties, metrics, identity);
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "worker.queue-mode", havingValue = "redis")
    static class RedisMode {

        @Bean
        RedisReadyQueue readyQueue(WorkerProperties properties) {
            return new RedisReadyQueue(properties.redisUrl());
        }

        @Bean
        ClaimSource claimSource(
                JobStore store,
                RedisReadyQueue readyQueue,
                WorkerProperties properties,
                WorkerMetrics metrics,
                WorkerIdentity identity) {
            return new RedisClaimSource(store, readyQueue, properties, metrics, identity);
        }

        @Bean
        DelayedJobPromoter delayedJobPromoter(RedisReadyQueue readyQueue) {
            return new DelayedJobPromoter(readyQueue);
        }
    }
}
