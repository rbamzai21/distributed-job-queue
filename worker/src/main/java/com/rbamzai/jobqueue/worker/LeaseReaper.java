package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.Job;
import com.rbamzai.jobqueue.common.JobStatus;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically recovers jobs whose lease expired, which is how a job survives its worker crashing
 * mid-run. Every worker runs a reaper, so recovery has no single point of failure.
 */
@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);
    private static final int BATCH_SIZE = 100;

    private final JobStore store;
    private final WorkerMetrics metrics;

    public LeaseReaper(JobStore store, WorkerMetrics metrics) {
        this.store = store;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${worker.reaper-interval}", initialDelayString = "${worker.reaper-interval}")
    public void reap() {
        try {
            List<Job> reaped;
            do {
                reaped = store.reapExpiredLeases(BATCH_SIZE);
                if (!reaped.isEmpty()) {
                    long dead = reaped.stream().filter(job -> job.status() == JobStatus.DEAD).count();
                    long requeued = reaped.size() - dead;
                    metrics.recordReaped(requeued, dead);
                    log.warn("Reaped {} job(s) with expired leases: {} requeued, {} dead-lettered",
                            reaped.size(), requeued, dead);
                }
            } while (reaped.size() == BATCH_SIZE);
        } catch (DataAccessException e) {
            log.warn("Reaper run failed, will retry: {}", e.getMessage());
        }
    }
}
