package dev.ninoer.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import dev.ninoer.model.QuakeEvent;
import dev.ninoer.ingest.EventParser;
import dev.ninoer.store.EventStore;
import dev.ninoer.store.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Wraps a KafkaConsumer<String, String>, reads quake.events.v1, and drives
 */
public class EventConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final EventStore store;
    private final EventProducer dltProducer;

    private volatile boolean running = true;

    public EventConsumer(String bootstrapServers, String groupId, EventStore store, EventProducer dltProducer) {
        Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                // manual commit: I only advance the offset after the record is actually in Postgres, not just after it's been read off the topic
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(EventProducer.TOPIC));
        this.store = store;
        this.dltProducer = dltProducer;
    }

    /** Runs the poll loop until stop() is called. */
    public void run() {
        // once true, stays true until a restart, a later successful batch must never
        // commit an offset that silently skips past an earlier one that failed to store
        boolean commitsPaused = false;
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    if (!handle(record)) {
                        commitsPaused = true;
                    }
                }
                if (!records.isEmpty() && !commitsPaused) {
                    consumer.commitSync();
                }
            }
        } finally {
            consumer.close();
        }
    }

    /** @return true if the record was fully handled */
    private boolean handle(ConsumerRecord<String, String> record) {
        QuakeEvent event;
        try {
            JsonNode feature = JsonObject.readTree(record.value());
            event = EventParser.parseFeature(feature);
        } catch (Exception e) {
            System.err.println("routing malformed record " + record.key() + " to DLT: " + e.getMessage());
            dltProducer.sendToDlt(record.key(), record.value());
            return true;
        }

        try {
            store.save(event);
            return true;
        } catch (SQLException e) {
            System.err.println("failed to store " + record.key() + ", will retry after restart: " + e.getMessage());
            return false;
        }
    }

    public void stop() {
        running = false;
    }
}
