package dev.ninoer.ingest;

/**
 * Thrown when a feature from the feed doesn't have the shape EventParser
 * expects. Checked on purpose: the caller must decide what to do with the
 * bad record (route it to quake.events.v1.DLT) rather than let it propagate.
 */
public class EventParseException extends Exception {
    public EventParseException(String message) {
        super(message);
    }

    public EventParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
