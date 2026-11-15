package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.RedisReadyQueue;
import io.lettuce.core.RedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Redis mode only: moves delayed jobs and retries whose run_at has passed into the ready set. Every
 * worker runs it; the move is one atomic Lua script, so concurrent promoters can't duplicate a job.
 */
public class DelayedJobPromoter {

    private static final Logger log = LoggerFactory.getLogger(DelayedJobPromoter.class);
    private static final int BATCH_SIZE = 500;

    private final RedisReadyQueue readyQueue;

    public DelayedJobPromoter(RedisReadyQueue readyQueue) {
        this.readyQueue = readyQueue;
    }

    @Scheduled(fixedDelayString = "${worker.promote-interval}")
    public void promote() {
        try {
            long moved;
            do {
                moved = readyQueue.promoteDue(BATCH_SIZE);
            } while (moved == BATCH_SIZE);
        } catch (RedisException e) {
            // RedisClaimSource already reports the outage; due jobs are caught by the Postgres sweep meanwhile.
            log.debug("Promotion skipped, Redis unavailable: {}", e.getMessage());
        }
    }
}
