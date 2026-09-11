package dev.ninoer.model;

import java.time.Instant;

/**
 * One quake, as of one revision from the feed. Mirrors quake_event_log /
 * quake_current (see docker/postgres/init/01-schema.sql) closely on purpose,
 * so EventStore can map fields 1:1 without any translation layer.
 */
// https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
public record QuakeEvent(
        String usgsId,
        Double magnitude, // query api
        String place, // query api
        String regionKey,   // not in the raw feed; computed downstream
        Double lon, // query api
        Double lat, // query api
        Double depthKm, // query api
        String status,
        Instant occurredAt,
        Instant updatedAt,
        String rawPayload   // the original GeoJSON feature, stored as-is into quake_event_log.payload
) { }