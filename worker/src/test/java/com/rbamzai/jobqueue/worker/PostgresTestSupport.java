package com.rbamzai.jobqueue.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobRowMapper;
import com.rbamzai.jobqueue.common.JobStatus;
import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One real Postgres for all integration tests, migrated with the production migrations from
 * db/migrations. Each test starts from an empty jobs table.
 */
abstract class PostgresTestSupport {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17");
    static final JobRowMapper MAPPER = new JobRowMapper(new ObjectMapper());

    static {
        POSTGRES.start(); // stopped by Testcontainers' Ryuk sidecar when the JVM exits
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("filesystem:../db/migrations")
                .load()
                .migrate();
    }

    HikariDataSource testDataSource;
    JdbcClient jdbc;

    @BeforeEach
    void setUpDatabase() {
        testDataSource = newDataSource();
        jdbc = JdbcClient.create(testDataSource);
        jdbc.sql("TRUNCATE jobs").update();
    }

    @AfterEach
    void closeDatabase() {
        testDataSource.close();
    }

    static HikariDataSource newDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(12);
        return dataSource;
    }

    UUID enqueue(String type) {
        return enqueue(type, 0, 5, 0);
    }

    UUID enqueue(String type, int priority, int maxAttempts, long delaySeconds) {
        return insertJob(type, priority, maxAttempts, delaySeconds).id();
    }

    /** Inserts a PENDING job, like the API does, and returns the row. */
    Job insertJob(String type, int priority, int maxAttempts, double delaySeconds) {
        return jdbc.sql("""
                        INSERT INTO jobs (type, priority, max_attempts, run_at)
                        VALUES (:type, :priority, :maxAttempts, now() + make_interval(secs => :delay))
                        RETURNING *
                        """)
                .param("type", type)
                .param("priority", priority)
                .param("maxAttempts", maxAttempts)
                .param("delay", delaySeconds)
                .query(MAPPER)
                .single();
    }

    Job job(UUID id) {
        return jdbc.sql("SELECT * FROM jobs WHERE id = :id").param("id", id).query(MAPPER).single();
    }

    List<Job> allJobs() {
        return jdbc.sql("SELECT * FROM jobs").query(MAPPER).list();
    }

    Map<JobStatus, Long> countByStatus() {
        return allJobs().stream().collect(Collectors.groupingBy(Job::status, Collectors.counting()));
    }
}
