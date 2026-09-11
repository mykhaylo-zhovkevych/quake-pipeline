package dev.ninoer.kafka;

import dev.ninoer.model.QuakeEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/** Wraps a KafkaProducer<String, String> and publishes to quake.events.v1. */
public class EventProducer implements AutoCloseable {

    public static final String TOPIC = "quake.events.v1";
    public static final String DLT_TOPIC = TOPIC + ".DLT";

    // Depedency from kafka's libary
    private final KafkaProducer<String, String> producer;

    public EventProducer(String bootstrapServers) {
        Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "quake-pipeline-producer");
        this.producer = new KafkaProducer<>(props);
    }

    /** Publishes one event to quake.events.v1, keyed by usgsId. */
    public void send(QuakeEvent event) {
        // ProducerRecord(String topic, K key, V value)
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.usgsId(), event.rawPayload());
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("failed to send " + event.usgsId() + ": " + exception.getMessage());
            }
        });
    }

    /** Publishes a raw record that failed to parse/store to the dead-letter topic, unchanged. */
    public void sendToDlt(String key, String rawValue) {
        producer.send(new ProducerRecord<>(DLT_TOPIC, key, rawValue));
    }

    @Override
    public void close() {
        producer.close();
    }
}
