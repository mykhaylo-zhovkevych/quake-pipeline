package dev.ninoer.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonObject {

    private static final ObjectMapper Mapper = new ObjectMapper();

    private Json() {
    }

    /** Parses a JSON string into a mutable tree. */
    public static JsonNode readTree(String json) throws JsonProcessingException {
        return Mapper.readTree(json);
    }

    /** Deserializes a JSON string straight into a Java type. */
    public static <T> T readValue(String json, Class<T> type) throws JsonProcessingException {
        return Mapper.readValue(json, type);
    }

    /** Serializes any object to its JSON string form. */
    public static String writeValue(Object value) throws JsonProcessingException {
        return Mapper.writeValueAsString(value);
    }
}
