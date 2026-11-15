package com.rbamzai.jobqueue.worker;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.rbamzai.jobqueue.common.Job;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.function.ToDoubleFunction;
import org.springframework.stereotype.Component;

/**
 * Every metric the worker publishes, in one place. Timers publish percentile histograms so Prometheus
 * can compute p50/p95/p99 across all workers with histogram_quantile, which per-process percentiles
 * can't be aggregated into.
 */
@Component
public class WorkerMetrics {

    public enum Outcome { SUCCEEDED, RETRIED, DEAD, LEASE_LOST }

    private final MeterRegistry registry;
    private final Timer queueWait;
    private final Counter leasesLost;

    public WorkerMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.queueWait = Timer.builder("jobqueue.job.queue.wait")
                .description("Time from when a job became runnable until a worker claimed it")
                .publishPercentileHistogram()
                .register(registry);
        this.leasesLost = Counter.builder("jobqueue.leases.lost")
                .description("Leases found reaped by this worker's heartbeat while it was still running the job")
                .register(registry);
    }

    /**
     * @param source postgres (polling), redis (claim by popped ids), or sweep (Redis mode's periodic
     *               Postgres claim); elapsed covers only the Postgres query, not time spent waiting for work
     */
    public void recordClaim(String source, List<Job> claimed, long elapsedNanos) {
        Timer.builder("jobqueue.claim.latency")
                .description("Duration of one claim query")
                .tag("source", source)
                .publishPercentileHistogram()
                .register(registry)
                .record(elapsedNanos, NANOSECONDS);
        DistributionSummary.builder("jobqueue.claim.batch.size")
                .description("Jobs returned per claim query")
                .tag("source", source)
                .register(registry)
                .record(claimed.size());
        for (Job job : claimed) {
            // updated_at was set by the claim, and both timestamps come from the database clock.
            Duration wait = Duration.between(job.runAt(), job.updatedAt());
            queueWait.record(wait.isNegative() ? Duration.ZERO : wait);
        }
    }

    public void recordExecution(String type, Outcome outcome, long elapsedNanos) {
        Timer.builder("jobqueue.job.execution")
                .description("Handler run time plus recording the outcome")
                .tag("type", type)
                .tag("outcome", outcome.name().toLowerCase(Locale.ROOT))
                .publishPercentileHistogram()
                .register(registry)
                .record(elapsedNanos, NANOSECONDS);
    }

    public void recordReaped(long requeued, long dead) {
        reapedCounter("requeued").increment(requeued);
        reapedCounter("dead").increment(dead);
    }

    public void recordLeasesLost(long count) {
        leasesLost.increment(count);
    }

    public <T> void registerInFlight(T source, ToDoubleFunction<T> inFlight) {
        Gauge.builder("jobqueue.jobs.inflight", source, inFlight)
                .description("Jobs currently executing in this worker")
                .register(registry);
    }

    private Counter reapedCounter(String outcome) {
        return Counter.builder("jobqueue.leases.reaped")
                .description("Expired leases recovered by the reaper")
                .tag("outcome", outcome)
                .register(registry);
    }
}
