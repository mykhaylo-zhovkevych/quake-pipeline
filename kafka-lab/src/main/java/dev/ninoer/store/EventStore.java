package dev.ninoer.store;

import dev.ninoer.model.QuakeEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * The only class in the project that talks JDBC/Postgres. One Connection, plain PreparedStatements
 */
public class EventStore implements AutoCloseable {

    private final Connection connection;

    public EventStore(Connection connection) {
        this.connection = connection;
    }

    public void save(QuakeEvent event) throws SQLException {
        boolean previousAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            insertLog(event);
            upsertCurrent(event);
            // One commit, covering both statements
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }

    public void insertLog(QuakeEvent event) throws SQLException {
        String sql = """
                INSERT INTO quake_event_log (usgs_id, updated_at, payload)
                VALUES (?, ?, ?::jsonb)
                ON CONFLICT (usgs_id, updated_at) DO NOTHING
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, event.usgsId());
            ps.setTimestamp(2, Timestamp.from(event.updatedAt()));
            ps.setString(3, event.rawPayload());
            ps.executeUpdate();
        }
    }

    /**
     * One row per usgsId: insert on first sight, overwrite in place on every
     * later revision (status change, magnitude revision, ...).
     */
    public void upsertCurrent(QuakeEvent event) throws SQLException {
        String sql = """
                INSERT INTO quake_current (usgs_id, magnitude, place, region_key, lon, lat, depth_km, status, occurred_at, updated_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (usgs_id) DO UPDATE SET
                    magnitude   = EXCLUDED.magnitude,
                    place       = EXCLUDED.place,
                    region_key  = EXCLUDED.region_key,
                    lon         = EXCLUDED.lon,
                    lat         = EXCLUDED.lat,
                    depth_km    = EXCLUDED.depth_km,
                    status      = EXCLUDED.status,
                    occurred_at = EXCLUDED.occurred_at,
                    updated_at  = EXCLUDED.updated_at
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, event.usgsId());
            ps.setObject(2, event.magnitude());
            ps.setString(3, event.place());
            ps.setString(4, event.regionKey());
            ps.setObject(5, event.lon());
            ps.setObject(6, event.lat());
            ps.setObject(7, event.depthKm());
            ps.setString(8, event.status());
            ps.setObject(9, event.occurredAt() == null ? null : Timestamp.from(event.occurredAt()));
            ps.setTimestamp(10, Timestamp.from(event.updatedAt()));
            ps.executeUpdate();
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
