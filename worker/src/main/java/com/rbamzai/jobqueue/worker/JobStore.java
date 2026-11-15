package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobRowMapper;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The worker's view of the jobs table. Every method is a single statement in its own transaction, and
 * all times come from the database clock, so workers never compare their own (possibly skewed) clocks.
 *
 * <p>Everything after the claim is fenced by {@code lease_id}: it only touches a row that is still
 * RUNNING under the lease the caller claimed. If that lease was reaped or released, the update matches
 * nothing and returns false, so a slow worker can never overwrite the outcome of whoever took over.
 */
@Repository
public class JobStore {

    private final JdbcClient jdbc;
    private final JobRowMapper mapper;

    public JobStore(JdbcClient jdbc, JobRowMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * Atomically claims up to {@code limit} ready jobs, highest priority first.
     *
     * <p>FOR UPDATE SKIP LOCKED is what lets many workers run this at once: rows another worker has
     * locked mid-claim are skipped instead of waited on, so no two workers ever get the same job.
     */
    public List<Job> claim(String workerId, int limit, Duration lease) {
        return jdbc.sql("""
                        WITH next AS (
                            SELECT id FROM jobs
                            WHERE status = 'PENDING' AND run_at <= now()
                            ORDER BY priority DESC, run_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE jobs j
                        SET status = 'RUNNING',
                            attempts = j.attempts + 1,
                            worker_id = :workerId,
                            lease_id = gen_random_uuid(),
                            lease_expires_at = now() + make_interval(secs => :leaseSeconds),
                            updated_at = now()
                        FROM next
                        WHERE j.id = next.id
                        RETURNING j.*
                        """)
                .param("limit", limit)
                .param("workerId", workerId)
                .param("leaseSeconds", seconds(lease))
                .query(mapper)
                .list();
    }

    /** Heartbeat: pushes out the expiry of every lease still held. Returns the lease ids that were extended. */
    public List<UUID> extendLeases(Collection<UUID> leaseIds, Duration lease) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET lease_expires_at = now() + make_interval(secs => :leaseSeconds), updated_at = now()
                        WHERE status = 'RUNNING' AND lease_id IN (:leaseIds)
                        RETURNING lease_id
                        """)
                .param("leaseSeconds", seconds(lease))
                .param("leaseIds", leaseIds)
                .query(UUID.class)
                .list();
    }

    /**
     * Claims specific jobs, the ones a worker just popped from the Redis ready set. Only rows that are
     * still PENDING and due are taken; stale ids (cancelled, already claimed via the Postgres sweep) are
     * silently skipped, which is what lets Redis be a best-effort index rather than a source of truth.
     */
    public List<Job> claimByIds(String workerId, Collection<UUID> ids, Duration lease) {
        return jdbc.sql("""
                        WITH next AS (
                            SELECT id FROM jobs
                            WHERE id IN (:ids) AND status = 'PENDING' AND run_at <= now()
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE jobs j
                        SET status = 'RUNNING',
                            attempts = j.attempts + 1,
                            worker_id = :workerId,
                            lease_id = gen_random_uuid(),
                            lease_expires_at = now() + make_interval(secs => :leaseSeconds),
                            updated_at = now()
                        FROM next
                        WHERE j.id = next.id
                        RETURNING j.*
                        """)
                .param("ids", ids)
                .param("workerId", workerId)
                .param("leaseSeconds", seconds(lease))
                .query(mapper)
                .list();
    }

    /** Hands leases back on shutdown so other workers can run the jobs now, not after the leases expire. */
    public List<Job> releaseLeases(Collection<UUID> leaseIds) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'PENDING', run_at = now(), worker_id = NULL, lease_id = NULL,
                            lease_expires_at = NULL, last_error = 'released by worker shutdown', updated_at = now()
                        WHERE status = 'RUNNING' AND lease_id IN (:leaseIds)
                        RETURNING *
                        """)
                .param("leaseIds", leaseIds)
                .query(mapper)
                .list();
    }

    /**
     * Recovers jobs whose worker stopped heartbeating (crashed, hung, or cut off from the database).
     * Jobs with attempts left go back to PENDING; the rest are dead-lettered, so a job that crashes its
     * worker every time (a "poison pill") can't take down the whole fleet forever.
     *
     * <p>Every worker runs this. SKIP LOCKED keeps concurrent reapers from blocking on each other, and
     * the status check means a row is only ever reaped once.
     */
    public List<Job> reapExpiredLeases(int limit) {
        return jdbc.sql("""
                        WITH expired AS (
                            SELECT id FROM jobs
                            WHERE status = 'RUNNING' AND lease_expires_at < now()
                            ORDER BY lease_expires_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE jobs j
                        SET status = CASE WHEN j.attempts >= j.max_attempts THEN 'DEAD' ELSE 'PENDING' END,
                            run_at = now(),
                            last_error = 'lease expired on worker ' || j.worker_id,
                            worker_id = NULL,
                            lease_id = NULL,
                            lease_expires_at = NULL,
                            updated_at = now()
                        FROM expired
                        WHERE j.id = expired.id
                        RETURNING j.*
                        """)
                .param("limit", limit)
                .query(mapper)
                .list();
    }

    public boolean markSucceeded(Job job) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'SUCCEEDED', lease_id = NULL, lease_expires_at = NULL, updated_at = now()
                        WHERE id = :id AND status = 'RUNNING' AND lease_id = :leaseId
                        """)
                        .param("id", job.id())
                        .param("leaseId", job.leaseId())
                        .update()
                == 1;
    }

    /**
     * Puts a failed job back in the queue, runnable again after {@code delay}. Returns the updated row,
     * or empty if this worker no longer holds the lease.
     */
    public Optional<Job> scheduleRetry(Job job, Duration delay, String error) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'PENDING',
                            run_at = now() + make_interval(secs => :delaySeconds),
                            worker_id = NULL,
                            lease_id = NULL,
                            lease_expires_at = NULL,
                            last_error = :error,
                            updated_at = now()
                        WHERE id = :id AND status = 'RUNNING' AND lease_id = :leaseId
                        RETURNING *
                        """)
                .param("id", job.id())
                .param("leaseId", job.leaseId())
                .param("delaySeconds", seconds(delay))
                .param("error", error)
                .query(mapper)
                .optional();
    }

    /** Moves a job to the dead-letter queue. */
    public boolean markDead(Job job, String error) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'DEAD', lease_id = NULL, lease_expires_at = NULL, last_error = :error,
                            updated_at = now()
                        WHERE id = :id AND status = 'RUNNING' AND lease_id = :leaseId
                        """)
                        .param("id", job.id())
                        .param("leaseId", job.leaseId())
                        .param("error", error)
                        .update()
                == 1;
    }

    private static double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
