package com.rbamzai.jobqueue.api;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobRowMapper;
import com.rbamzai.jobqueue.common.JobStatus;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JobRepository {

    private final JdbcClient jdbc;
    private final JobRowMapper mapper;

    public JobRepository(JdbcClient jdbc, JobRowMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * Inserts a PENDING job, or returns empty if a job with this idempotency key already exists.
     *
     * <p>ON CONFLICT makes the duplicate check and the insert one atomic step, so two concurrent
     * requests with the same key can't both create a job. run_at uses the database clock so API and
     * worker clocks can't disagree.
     */
    public Optional<Job> insert(
            String type, String payloadJson, int priority, int maxAttempts, long delaySeconds, String idempotencyKey) {
        return jdbc.sql("""
                        INSERT INTO jobs (type, payload, priority, max_attempts, run_at, idempotency_key)
                        VALUES (:type, CAST(:payload AS jsonb), :priority, :maxAttempts,
                                now() + make_interval(secs => :delaySeconds), :idempotencyKey)
                        ON CONFLICT (idempotency_key) DO NOTHING
                        RETURNING *
                        """)
                .param("type", type)
                .param("payload", payloadJson)
                .param("priority", priority)
                .param("maxAttempts", maxAttempts)
                .param("delaySeconds", (double) delaySeconds)
                .param("idempotencyKey", idempotencyKey, Types.VARCHAR)
                .query(mapper)
                .optional();
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.sql("SELECT * FROM jobs WHERE id = :id")
                .param("id", id)
                .query(mapper)
                .optional();
    }

    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        return jdbc.sql("SELECT * FROM jobs WHERE idempotency_key = :key")
                .param("key", idempotencyKey)
                .query(mapper)
                .optional();
    }

    public List<Job> findRecent(int limit) {
        return jdbc.sql("SELECT * FROM jobs ORDER BY created_at DESC LIMIT :limit")
                .param("limit", limit)
                .query(mapper)
                .list();
    }

    public List<Job> findRecentByStatus(JobStatus status, int limit) {
        return jdbc.sql("SELECT * FROM jobs WHERE status = :status ORDER BY created_at DESC LIMIT :limit")
                .param("status", status.name())
                .param("limit", limit)
                .query(mapper)
                .list();
    }

    /** Cancels a job that hasn't started. Empty if the job doesn't exist or isn't PENDING. */
    public Optional<Job> cancelIfPending(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'CANCELLED', updated_at = now()
                        WHERE id = :id AND status = 'PENDING'
                        RETURNING *
                        """)
                .param("id", id)
                .query(mapper)
                .optional();
    }

    /**
     * Moves a dead-lettered job back to the queue with a fresh attempt budget. Empty if the job doesn't
     * exist or isn't DEAD. Resetting attempts is safe because ownership is fenced by lease_id, not attempts.
     */
    public Optional<Job> requeueIfDead(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'PENDING', attempts = 0, run_at = now(), updated_at = now()
                        WHERE id = :id AND status = 'DEAD'
                        RETURNING *
                        """)
                .param("id", id)
                .query(mapper)
                .optional();
    }
}
