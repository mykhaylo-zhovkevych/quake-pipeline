package dev.ninoer;

import dev.ninoer.config.AppConfig;
import dev.ninoer.ingest.EventParseException;
import dev.ninoer.ingest.EventParser;
import dev.ninoer.ingest.UsgsClient;
import dev.ninoer.kafka.EventConsumer;
import dev.ninoer.kafka.EventProducer;
import dev.ninoer.model.QuakeEvent;
import dev.ninoer.store.EventStore;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * Part 1 smoke test: fetch the USGS feed once, parse it, print what came back.
 */
public class App {
    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnv();

        UsgsClient usgsClient = new UsgsClient(URI.create(config.usgsFeedUrl()));
        String feedBody = usgsClient.fetchFeed();

        List<QuakeEvent> events;
        try {
            events = EventParser.parseFeed(feedBody);
        } catch (EventParseException e) {
            System.err.println("failed to parse feed: " + e.getMessage());
            return;
        }

        // produce: publish every event onto quake.events.v1
        try (EventProducer producer = new EventProducer(config.kafkaBootstrap())) {
            for (QuakeEvent event : events) {
                producer.send(event);
            }
        }
        // consume + store: read them back, persist to Postgres
        try (Connection connection = DriverManager.getConnection(config.dbUrl(), config.dbUser(), config.dbPassword());
             EventStore store = new EventStore(connection);
             EventProducer dltProducer = new EventProducer(config.kafkaBootstrap())) {

            EventConsumer consumer = new EventConsumer(config.kafkaBootstrap(), "quake-pipeline", store, dltProducer);
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop));
            System.out.println("consuming " + EventProducer.TOPIC + " - Ctrl+C to stop");
            consumer.run();
        }
    }
}
