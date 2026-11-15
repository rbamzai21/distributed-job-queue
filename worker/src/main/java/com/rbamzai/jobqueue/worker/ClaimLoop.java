package com.rbamzai.jobqueue.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rbamzai.jobqueue.common.Job;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * Claims ready jobs from a {@link ClaimSource} and runs each one on its own virtual thread.
 *
 * <p>A semaphore caps in-flight jobs at {@code worker.concurrency}. The loop only claims as many jobs
 * as it has free slots, so a busy worker leaves the rest of the queue for other workers instead of
 * hoarding jobs it can't start yet.
 */
@Component
public class ClaimLoop implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ClaimLoop.class);

    private final ClaimSource source;
    private final JobRunner runner;
    private final LeaseKeeper leases;
    private final WorkerProperties properties;
    private final String workerId;
    private final Semaphore slots;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final CountDownLatch stopRequested = new CountDownLatch(1);

    private volatile boolean running;
    private Thread loopThread;

    public ClaimLoop(
            ClaimSource source,
            JobRunner runner,
            LeaseKeeper leases,
            WorkerProperties properties,
            WorkerIdentity identity) {
        this.source = source;
        this.runner = runner;
        this.leases = leases;
        this.properties = properties;
        this.workerId = identity.id();
        this.slots = new Semaphore(properties.concurrency());
    }

    @Override
    public void start() {
        running = true;
        leases.start();
        loopThread = Thread.ofPlatform().name("claim-loop").start(this::loop);
        log.info("Worker {} started (mode={}, concurrency={}, batchSize={})",
                workerId, properties.queueMode(), properties.concurrency(), properties.batchSize());
    }

    /**
     * Graceful shutdown: stop claiming, give in-flight jobs time to finish (still heartbeating), then
     * release the leases of any stragglers so other workers can pick them up right away.
     */
    @Override
    public void stop() {
        log.info("Worker {} stopping: no new claims, waiting up to {} for in-flight jobs",
                workerId, properties.shutdownTimeout());
        running = false;
        stopRequested.countDown();
        try {
            loopThread.join();
            executor.shutdown();
            if (!executor.awaitTermination(properties.shutdownTimeout().toMillis(), MILLISECONDS)) {
                int released = leases.releaseAll();
                log.warn("Released {} lease(s) for jobs that did not finish in time; other workers will rerun them",
                        released);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (DataAccessException e) {
            log.warn("Could not release leases on shutdown; they will be reaped when they expire: {}", e.getMessage());
        } finally {
            leases.stop();
        }
        log.info("Worker {} stopped", workerId);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        try {
            while (running) {
                int free = awaitFreeSlots();
                if (free == 0) {
                    continue; // stopping
                }
                List<Job> claimed;
                try {
                    claimed = source.claim(Math.min(free, properties.batchSize()));
                } catch (DataAccessException e) {
                    slots.release(free);
                    log.warn("Claim query failed, will retry: {}", e.getMessage());
                    pause();
                    continue;
                }
                slots.release(free - claimed.size());
                claimed.forEach(this::dispatch);
                if (claimed.isEmpty() && !source.waitsForWork()) {
                    pause();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Blocks until at least one slot is free, then takes every free slot. Returns 0 if stopping. */
    private int awaitFreeSlots() throws InterruptedException {
        while (running) {
            if (slots.tryAcquire(properties.pollInterval().toMillis(), MILLISECONDS)) {
                return 1 + slots.drainPermits();
            }
        }
        return 0;
    }

    private void dispatch(Job job) {
        leases.track(job);
        executor.execute(() -> {
            try {
                runner.run(job);
            } catch (RuntimeException e) {
                log.error("Could not record the outcome of job {}; it stays RUNNING until its lease expires",
                        job.id(), e);
            } finally {
                leases.untrack(job);
                slots.release();
            }
        });
    }

    /** Sleeps for one poll interval, waking early if shutdown starts. */
    private void pause() throws InterruptedException {
        stopRequested.await(properties.pollInterval().toMillis(), MILLISECONDS);
    }
}
