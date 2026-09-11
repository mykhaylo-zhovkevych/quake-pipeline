package dev.ninoer.config;

/**
 * Everything the app needs, read from the environment (see kafka-lab/Dockerfile
 * for the container defaults). Each field falls back to a sane local-dev value
 * so `mvn exec:java` works without a .env file.
 */
public record AppConfig(
        String kafkaBootstrap,
        String dbUrl,
        String dbUser,
        String dbPassword,
        String usgsFeedUrl,
        int pollIntervalSeconds
) {
    public static AppConfig fromEnv() {
        return new AppConfig(
                env("KAFKA_BOOTSTRAP", "localhost:19092"),
                env("DB_URL", "jdbc:postgresql://localhost:5433/events"),
                env("DB_USER", "app"),
                env("DB_PASSWORD", "app"),
                env("USGS_FEED_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"),
                Integer.parseInt(env("POLL_INTERVAL_SECONDS", "60"))
        );
    }

    // Simple fallback protection
    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}