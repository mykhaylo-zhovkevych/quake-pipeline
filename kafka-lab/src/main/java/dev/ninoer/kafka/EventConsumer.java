package dev.ninoer.kafka;

public class EventConsumer {

    public EventConsumer(String bootstrapServers, String groupId) {
        // TODO: build Properties, construct KafkaConsumer<String, String>, subscribe
    }

    /** Runs the poll loop until interrupted/shut down. */
    public void run() {
        // TODO: while (running) { poll -> for each record: parse, store or DLT -> commit }
    }
}
