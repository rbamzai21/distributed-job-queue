package com.rbamzai.jobqueue.worker;

import com.rbamzai.jobqueue.common.Job;
import java.util.List;

/** Where the claim loop gets its next jobs: Postgres polling or the Redis fast path. */
public interface ClaimSource {

    /** Claims up to {@code max} jobs; returns empty when nothing is ready. May block briefly. */
    List<Job> claim(int max) throws InterruptedException;

    /** True if {@link #claim} already waited for work, so the loop shouldn't sleep again on an empty result. */
    boolean waitsForWork();
}
