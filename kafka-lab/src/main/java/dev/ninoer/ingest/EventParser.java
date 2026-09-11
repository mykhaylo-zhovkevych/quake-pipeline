package dev.ninoer.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ninoer.model.QuakeEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON (a USGS GeoJSON FeatureCollection) -> QuakeEvent. Throws on bad
 * shape; the caller is responsible for routing the offending record to
 * quake.events.v1.DLT instead of crashing.
 *
 * Static utility class on purpose — no instance state, so no instances.
 */
public final class EventParser {

    private static final ObjectMapper Mapper = new ObjectMapper();

    private EventParser() {
    }

    /** Parses a full FeatureCollection response body into one QuakeEvent per feature. */
    public static List<QuakeEvent> parseFeed(String geoJson) throws EventParseException {
        JsonNode root;
        try {
            root = Mapper.readTree(geoJson);
        } catch (Exception e) {
            throw new EventParseException("feed body is not valid JSON", e);
        }

        JsonNode features = root.get("features");
        if (features == null || !features.isArray()) {
            throw new EventParseException("expected a GeoJSON FeatureCollection with a \"features\" array");
        }

        List<QuakeEvent> events = new ArrayList<>();
        for (JsonNode feature : features) {
            events.add(parseFeature(feature));
        }
        return events;
    }

    /** Parses a single GeoJSON Feature into a QuakeEvent. */
    public static QuakeEvent parseFeature(JsonNode feature) throws EventParseException {
        try {
            String usgsId = require(feature, "id").asText();
            JsonNode props = require(feature, "properties");
            JsonNode geometry = require(feature, "geometry");
            JsonNode coords = require(geometry, "coordinates");
            if (!coords.isArray() || coords.size() < 3) {
                throw new EventParseException("geometry.coordinates must be [lon, lat, depth]: " + usgsId);
            }

            if (!props.hasNonNull("updated")) {
                throw new EventParseException("missing required field: properties.updated for " + usgsId);
            }

            return new QuakeEvent(
                    usgsId,
                    props.hasNonNull("mag") ? props.get("mag").asDouble() : null,
                    props.hasNonNull("place") ? props.get("place").asText() : null,
                    null, // regionKey: not in the raw feed, computed downstream
                    coords.get(0).asDouble(),
                    coords.get(1).asDouble(),
                    coords.get(2).asDouble(),
                    props.hasNonNull("status") ? props.get("status").asText() : null,
                    props.hasNonNull("time") ? Instant.ofEpochMilli(props.get("time").asLong()) : null,
                    Instant.ofEpochMilli(props.get("updated").asLong()),
                    feature.toString()
            );
        } catch (EventParseException e) {
            throw e;
        } catch (Exception e) {
            throw new EventParseException("malformed feature", e);
        }
    }

    private static JsonNode require(JsonNode node, String field) throws EventParseException {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new EventParseException("missing required field: " + field);
        }
        return value;
    }
}
