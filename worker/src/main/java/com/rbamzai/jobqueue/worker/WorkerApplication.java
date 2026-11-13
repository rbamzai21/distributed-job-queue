package com.rbamzai.jobqueue.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbamzai.jobqueue.common.Backoff;
import com.rbamzai.jobqueue.common.JobRowMapper;
import java.util.Random;
import java.util.UUID;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

    @Bean
    JobRowMapper jobRowMapper(ObjectMapper objectMapper) {
        return new JobRowMapper(objectMapper);
    }

    @Bean
    Backoff backoff(WorkerProperties properties) {
        // java.util.Random is thread-safe, which matters since every job runs on its own thread.
        return new Backoff(properties.backoffBase(), properties.backoffMax(), new Random());
    }

    @Bean
    WorkerIdentity workerIdentity(WorkerProperties properties) {
        if (properties.id() != null) {
            return new WorkerIdentity(properties.id());
        }
        String host = System.getenv().getOrDefault("HOSTNAME", "worker");
        return new WorkerIdentity(host + "-" + UUID.randomUUID().toString().substring(0, 8));
    }
}
