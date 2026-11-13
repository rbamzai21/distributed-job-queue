package com.rbamzai.jobqueue.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param id                unique name for this worker process; defaults to hostname plus a random suffix
 * @param concurrency       max jobs running at once in this process
 * @param batchSize         max jobs claimed per query
 * @param pollInterval      how long to wait before polling again when the queue is empty
 * @param leaseDuration     how long a claim stays valid without a heartbeat
 * @param heartbeatInterval how often held leases are extended; keep well under leaseDuration
 * @param reaperInterval    how often expired leases are recovered
 * @param shutdownTimeout   how long to wait for in-flight jobs on shutdown before releasing their leases
 */
@ConfigurationProperties("worker")
public record WorkerProperties(
        String id,
        @DefaultValue("8") int concurrency,
        @DefaultValue("10") int batchSize,
        @DefaultValue("500ms") Duration pollInterval,
        @DefaultValue("30s") Duration leaseDuration,
        @DefaultValue("10s") Duration heartbeatInterval,
        @DefaultValue("5s") Duration reaperInterval,
        @DefaultValue("1s") Duration backoffBase,
        @DefaultValue("5m") Duration backoffMax,
        @DefaultValue("30s") Duration shutdownTimeout) {

    public WorkerProperties {
        if (heartbeatInterval.compareTo(leaseDuration.dividedBy(2)) > 0) {
            throw new IllegalArgumentException("worker.heartbeat-interval (" + heartbeatInterval
                    + ") must be at most half of worker.lease-duration (" + leaseDuration + ")");
        }
    }
}
