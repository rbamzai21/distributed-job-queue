package com.rbamzai.jobqueue.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsController {

    private final QueueStats stats;

    public StatsController(QueueStats stats) {
        this.stats = stats;
    }

    /** Queue depth by status, recent throughput and latency percentiles. Refreshed every few seconds. */
    @GetMapping("/stats")
    public QueueStats.Snapshot stats() {
        return stats.latest();
    }
}
