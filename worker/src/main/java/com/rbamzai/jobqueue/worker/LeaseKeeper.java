package com.rbamzai.jobqueue.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rbamzai.jobqueue.common.Job;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * Tracks the leases this worker holds and keeps them alive with one batched heartbeat query.
 *
 * <p>It heartbeats only leases whose jobs are actually executing here, not "everything with my worker
 * id": if a job's thread died without recording an outcome, its lease is left to expire so the reaper
 * can recover it, instead of being kept alive forever.
 *
 * <p>Runs on its own platform thread so a busy job pool can never starve the heartbeat.
 */
@Component
public class LeaseKeeper {

    private static final Logger log = LoggerFactory.getLogger(LeaseKeeper.class);

    private final JobStore store;
    private final WorkerProperties properties;
    private final Set<UUID> heldLeases = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("lease-heartbeat").factory());

    public LeaseKeeper(JobStore store, WorkerProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    public void start() {
        long intervalMillis = properties.heartbeatInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::heartbeat, intervalMillis, intervalMillis, MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    public void track(Job job) {
        heldLeases.add(job.leaseId());
    }

    public void untrack(Job job) {
        heldLeases.remove(job.leaseId());
    }

    /** Releases every lease still held. Called on shutdown for jobs that didn't finish in time. */
    public int releaseAll() {
        List<UUID> leases = List.copyOf(heldLeases);
        return leases.isEmpty() ? 0 : store.releaseLeases(leases);
    }

    private void heartbeat() {
        List<UUID> leases = List.copyOf(heldLeases);
        if (leases.isEmpty()) {
            return;
        }
        try {
            Set<UUID> extended = Set.copyOf(store.extendLeases(leases, properties.leaseDuration()));
            // A job that finished between the snapshot and the update is no longer held, so it isn't lost.
            long lost = leases.stream().filter(id -> !extended.contains(id) && heldLeases.contains(id)).count();
            if (lost > 0) {
                log.warn("{} lease(s) were reaped before this heartbeat; those jobs may run again elsewhere", lost);
            }
        } catch (DataAccessException e) {
            // Keep going: leases are long enough to survive a few missed heartbeats.
            log.warn("Heartbeat failed for {} lease(s): {}", leases.size(), e.getMessage());
        } catch (RuntimeException e) {
            // An exception escaping a scheduled task cancels all future runs, so never let one out.
            log.error("Unexpected heartbeat failure", e);
        }
    }
}
