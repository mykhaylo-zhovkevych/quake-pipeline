package dev.ninoer.kafka;

import dev.ninoer.model.QuakeEvent;

public class EventProducer implements AutoCloseable {

    public EventProducer(String bootstrapServers) {
        // TODO: build Properties, construct KafkaProducer<String, String>


    }

    /** Publishes one event to quake.events.v1, keyed by usgsId. */
    public void send(QuakeEvent event) {
        // TODO: build a ProducerRecord, call producer.send(...)
    }

    @Override
    public void close() {
        // TODO: producer.close()
    }
}
