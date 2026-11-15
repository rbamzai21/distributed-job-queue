package com.rbamzai.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The claim, lease and fencing SQL, tested directly against Postgres. */
class JobStoreIT extends PostgresTestSupport {

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration SHORT_LEASE = Duration.ofMillis(200);

    private JobStore store;

    @BeforeEach
    void setUp() {
        store = new JobStore(jdbc, MAPPER);
    }

    @Test
    void claimsHighestPriorityFirstAndSkipsJobsNotYetDue() {
        UUID low = enqueue("t", 0, 5, 0);
        UUID high = enqueue("t", 10, 5, 0);
        enqueue("t", 100, 5, 3600); // highest priority, but delayed an hour

        List<Job> claimed = store.claim("w1", 10, LEASE);

        assertThat(claimed).extracting(Job::id).containsExactlyInAnyOrder(low, high);
        assertThat(store.claim("w1", 1, LEASE)).isEmpty();
    }

    @Test
    void claimRespectsPriorityOrderWithinTheLimit() {
        enqueue("t", 0, 5, 0);
        UUID high = enqueue("t", 10, 5, 0);

        assertThat(store.claim("w1", 1, LEASE)).extracting(Job::id).containsExactly(high);
    }

    @Test
    void claimSetsLeaseAndCountsTheAttempt() {
        UUID id = enqueue("t");

        Job claimed = store.claim("w1", 1, LEASE).getFirst();

        assertThat(claimed.id()).isEqualTo(id);
        assertThat(claimed.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(claimed.attempts()).isEqualTo(1);
        assertThat(claimed.workerId()).isEqualTo("w1");
        assertThat(claimed.leaseId()).isNotNull();
        assertThat(claimed.leaseExpiresAt()).isAfter(claimed.updatedAt());
    }

    /** SKIP LOCKED: a row locked by another transaction is skipped, not waited on. */
    @Test
    void claimSkipsRowsLockedByAnotherTransactionInsteadOfBlocking() throws Exception {
        UUID locked = enqueue("t");
        UUID free = enqueue("t");

        try (Connection other = testDataSource.getConnection()) {
            other.setAutoCommit(false);
            try (PreparedStatement lock = other.prepareStatement("SELECT 1 FROM jobs WHERE id = ? FOR UPDATE")) {
                lock.setObject(1, locked);
                lock.executeQuery();
            }

            long start = System.nanoTime();
            List<Job> claimed = store.claim("w1", 10, LEASE);

            assertThat(claimed).extracting(Job::id).containsExactly(free);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
            other.rollback();
        }

        assertThat(store.claim("w1", 10, LEASE)).extracting(Job::id).containsExactly(locked);
    }

    /**
     * The fencing scenario: worker A stalls past its lease, the job is reaped and reclaimed by B, then A
     * wakes up. A's lease_id no longer matches, so it can neither heartbeat nor overwrite B's result.
     */
    @Test
    void staleWorkerCannotHeartbeatOrFinishAJobThatWasReclaimed() throws InterruptedException {
        UUID id = enqueue("t");
        Job claimedByA = store.claim("A", 1, SHORT_LEASE).getFirst();
        Thread.sleep(SHORT_LEASE.toMillis() * 2);

        assertThat(store.reapExpiredLeases(10)).extracting(Job::id).containsExactly(id);
        Job claimedByB = store.claim("B", 1, LEASE).getFirst();

        assertThat(store.extendLeases(List.of(claimedByA.leaseId()), LEASE)).isEmpty();
        assertThat(store.markSucceeded(claimedByA)).isFalse();
        assertThat(store.markDead(claimedByA, "stale")).isFalse();
        assertThat(job(id).workerId()).isEqualTo("B");

        assertThat(store.markSucceeded(claimedByB)).isTrue();
        assertThat(job(id).status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(job(id).attempts()).isEqualTo(2);
    }

    /** The same worker reclaiming the job is the case worker_id fencing alone would miss. */
    @Test
    void fencingHoldsEvenWhenTheSameWorkerReclaimsTheJob() throws InterruptedException {
        enqueue("t");
        Job firstClaim = store.claim("A", 1, SHORT_LEASE).getFirst();
        Thread.sleep(SHORT_LEASE.toMillis() * 2);
        store.reapExpiredLeases(10);
        Job secondClaim = store.claim("A", 1, LEASE).getFirst();

        assertThat(store.markSucceeded(firstClaim)).isFalse();
        assertThat(store.markSucceeded(secondClaim)).isTrue();
    }

    @Test
    void heartbeatKeepsALeaseFromBeingReaped() throws InterruptedException {
        enqueue("t");
        Job claimed = store.claim("A", 1, Duration.ofMillis(400)).getFirst();

        for (int i = 0; i < 4; i++) {
            Thread.sleep(200);
            assertThat(store.extendLeases(List.of(claimed.leaseId()), Duration.ofMillis(400)))
                    .containsExactly(claimed.leaseId());
        }

        assertThat(store.reapExpiredLeases(10)).isEmpty();
        assertThat(store.markSucceeded(claimed)).isTrue();
    }

    @Test
    void reaperDeadLettersJobsThatAreOutOfAttempts() throws InterruptedException {
        UUID poisonPill = enqueue("t", 0, 1, 0);
        store.claim("A", 1, SHORT_LEASE);
        Thread.sleep(SHORT_LEASE.toMillis() * 2);

        assertThat(store.reapExpiredLeases(10)).extracting(Job::status).containsExactly(JobStatus.DEAD);
        assertThat(job(poisonPill).lastError()).startsWith("lease expired on worker A");
    }

    @Test
    void releasedLeasesAreImmediatelyClaimableByOthers() {
        UUID id = enqueue("t");
        Job claimed = store.claim("A", 1, LEASE).getFirst();

        assertThat(store.releaseLeases(List.of(claimed.leaseId()))).isEqualTo(1);

        assertThat(store.claim("B", 1, LEASE)).extracting(Job::id).containsExactly(id);
        assertThat(store.markSucceeded(claimed)).isFalse();
    }
}
