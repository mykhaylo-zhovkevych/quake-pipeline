package dev.ninoer;

import dev.ninoer.config.AppConfig;
import dev.ninoer.ingest.EventParseException;
import dev.ninoer.ingest.EventParser;
import dev.ninoer.ingest.UsgsClient;
import dev.ninoer.model.QuakeEvent;

import java.net.URI;
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

        // fetched 9 events from https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
        //System.out.println("fetched " + events.size() + " events from " + config.usgsFeedUrl());
        events.stream().limit(5).forEach(System.out::println);
    }
}
