package com.rbamzai.jobqueue.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbamzai.jobqueue.common.JobRowMapper;
import com.rbamzai.jobqueue.common.QueueMode;
import com.rbamzai.jobqueue.common.ReadyQueue;
import com.rbamzai.jobqueue.common.RedisReadyQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }

    @Bean
    JobRowMapper jobRowMapper(ObjectMapper objectMapper) {
        return new JobRowMapper(objectMapper);
    }

    /** In Redis mode, newly runnable jobs are indexed in Redis so idle workers wake immediately. */
    @Bean
    ReadyQueue readyQueue(
            @Value("${jobqueue.queue-mode}") QueueMode mode, @Value("${jobqueue.redis-url}") String redisUrl) {
        return mode == QueueMode.REDIS ? new RedisReadyQueue(redisUrl) : ReadyQueue.NONE;
    }
}
