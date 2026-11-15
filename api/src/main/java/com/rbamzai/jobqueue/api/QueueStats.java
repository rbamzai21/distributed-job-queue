package com.rbamzai.jobqueue.api;

import com.rbamzai.jobqueue.common.JobStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Queue-wide stats computed from Postgres every few seconds and cached.
 *
 * <p>GET /stats and the Prometheus depth gauges both read the cached snapshot, so a dashboard polling
 * every second (or many dashboards) costs the database nothing extra.
 */
@Component
public class QueueStats {

    static final int LATENCY_WINDOW_SECONDS = 300;

    private final JdbcClient jdbc;
    private volatile Snapshot latest = Snapshot.empty();

    public QueueStats(JdbcClient jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        for (JobStatus status : JobStatus.values()) {
            Gauge.builder("jobqueue.jobs.depth", this, stats -> stats.latest.depth().getOrDefault(status, 0L))
                    .description("Jobs in each status, refreshed every few seconds")
                    .tag("status", status.name().toLowerCase(Locale.ROOT))
                    .register(registry);
        }
        Gauge.builder("jobqueue.jobs.oldest.ready.age", this, stats -> stats.latest.oldestReadyAgeSeconds())
                .description("Seconds the oldest runnable PENDING job has been waiting; grows when workers fall behind")
                .baseUnit("seconds")
                .register(registry);
    }

    /**
     * @param depth                  jobs per status
     * @param ready                  PENDING jobs whose run_at has passed (excludes delayed and backing-off jobs)
     * @param oldestReadyAgeSeconds  how long the oldest ready job has waited; 0 when none are waiting
     * @param lastMinute             jobs that finished in the last 60 seconds
     * @param latencyMs              enqueue-to-success latency over the last {@value #LATENCY_WINDOW_SECONDS}s,
     *                               including retries and any requested delay; null fields when nothing finished
     */
    public record Snapshot(
            Instant asOf,
            Map<JobStatus, Long> depth,
            long ready,
            double oldestReadyAgeSeconds,
            Throughput lastMinute,
            Latency latencyMs) {

        static Snapshot empty() {
            return new Snapshot(null, Collections.emptyMap(), 0, 0, new Throughput(0, 0), new Latency(null, null, null));
        }
    }

    public record Throughput(long succeeded, long dead) {}

    public record Latency(Double p50, Double p95, Double p99) {}

    public Snapshot latest() {
        return latest;
    }

    @Scheduled(initialDelay = 0, fixedDelayString = "${jobqueue.stats-refresh-interval:5s}")
    public void refresh() {
        try {
            latest = compute();
        } catch (DataAccessException e) {
            // Keep serving the previous snapshot; asOf shows how stale it is.
        }
    }

    private Snapshot compute() {
        Map<JobStatus, Long> depth = new EnumMap<>(JobStatus.class);
        for (JobStatus status : JobStatus.values()) {
            depth.put(status, 0L);
        }
        // A full count. Fine at this scale; at millions of rows this would move to counters kept by
        // the API/workers or to pg_class estimates for the finished statuses.
        jdbc.sql("SELECT status, count(*) AS n FROM jobs GROUP BY status")
                .query((rs, rowNum) -> Map.entry(JobStatus.valueOf(rs.getString("status")), rs.getLong("n")))
                .list()
                .forEach(entry -> depth.put(entry.getKey(), entry.getValue()));

        record Ready(long count, double oldestAgeSeconds) {}
        Ready ready = jdbc.sql("""
                        SELECT count(*) AS n, coalesce(extract(epoch FROM now() - min(run_at)), 0) AS oldest
                        FROM jobs WHERE status = 'PENDING' AND run_at <= now()
                        """)
                .query((rs, rowNum) -> new Ready(rs.getLong("n"), rs.getDouble("oldest")))
                .single();

        Throughput lastMinute = jdbc.sql("""
                        SELECT count(*) FILTER (WHERE status = 'SUCCEEDED') AS succeeded,
                               count(*) FILTER (WHERE status = 'DEAD') AS dead
                        FROM jobs
                        WHERE status IN ('SUCCEEDED', 'DEAD') AND updated_at > now() - interval '1 minute'
                        """)
                .query((rs, rowNum) -> new Throughput(rs.getLong("succeeded"), rs.getLong("dead")))
                .single();

        Latency latency = jdbc.sql("""
                        SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY ms) AS p50,
                               percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95,
                               percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99
                        FROM (SELECT extract(epoch FROM updated_at - created_at) * 1000 AS ms
                              FROM jobs
                              WHERE status = 'SUCCEEDED' AND updated_at > now() - make_interval(secs => :window)) finished
                        """)
                .param("window", (double) LATENCY_WINDOW_SECONDS)
                .query((rs, rowNum) -> new Latency(
                        rs.getObject("p50", Double.class),
                        rs.getObject("p95", Double.class),
                        rs.getObject("p99", Double.class)))
                .single();

        return new Snapshot(Instant.now(), depth, ready.count(), ready.oldestAgeSeconds(), lastMinute, latency);
    }
}
