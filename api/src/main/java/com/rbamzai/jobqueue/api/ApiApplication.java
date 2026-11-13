package com.rbamzai.jobqueue.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbamzai.jobqueue.common.JobRowMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }

    @Bean
    JobRowMapper jobRowMapper(ObjectMapper objectMapper) {
        return new JobRowMapper(objectMapper);
    }
}
