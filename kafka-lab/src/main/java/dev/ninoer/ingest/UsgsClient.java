package dev.ninoer.ingest;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Fetches the raw GeoJSON body from the USGS summary feed. One call = one
 * snapshot of the feed's current state
 */
public class UsgsClient {

    private final HttpClient http;
    private final URI feedUrl;

    // This is the default constructor
    public UsgsClient(URI feedUrl) {
        this(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(), feedUrl);
    }

    public UsgsClient(HttpClient http, URI feedUrl) {
        this.http = http;
        this.feedUrl = feedUrl;
    }

    /** @return the raw GeoJSON response body as text. */
    public String fetchFeed() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(feedUrl)
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("USGS feed returned HTTP " + response.statusCode() + ": " + feedUrl);
        }
        return response.body();
    }
}
