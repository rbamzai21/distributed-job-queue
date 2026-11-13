package com.rbamzai.jobqueue.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;

/** Maps a full {@code jobs} row (SELECT * / RETURNING *) to a {@link Job}. */
public final class JobRowMapper implements RowMapper<Job> {

    private final ObjectMapper objectMapper;

    public JobRowMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Job mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                readPayload(rs.getString("payload")),
                rs.getInt("priority"),
                JobStatus.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                instant(rs, "run_at"),
                instant(rs, "lease_expires_at"),
                rs.getObject("lease_id", UUID.class),
                rs.getString("worker_id"),
                rs.getString("idempotency_key"),
                rs.getString("last_error"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"));
    }

    private JsonNode readPayload(String json) throws SQLException {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new SQLException("jobs.payload is not valid JSON", e);
        }
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
