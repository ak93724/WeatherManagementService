package com.weather.system.wms.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.system.wms.processor.model.WeatherEvent;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class OpenMeteoClient {
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final OpenMeteoProperties properties;

    public OpenMeteoClient(ObjectMapper objectMapper, OpenMeteoProperties properties) {
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public WeatherEvent fetch(OpenMeteoProperties.Location location) throws IOException, InterruptedException {
        if (location.getId() == null || location.getId().isBlank()
                || location.getRegion() == null || location.getRegion().isBlank()
                || !Double.isFinite(location.getLatitude()) || !Double.isFinite(location.getLongitude())) {
            throw new IllegalArgumentException("Open-Meteo location must have an id, region, and finite coordinates");
        }
        String url = properties.getEndpoint() + "?latitude=" + location.getLatitude()
                + "&longitude=" + location.getLongitude()
                + "&current=temperature_2m,relative_humidity_2m&timezone=UTC";
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response = null;
        int attempts = Math.max(1, properties.getMaxRetries() + 1);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 == 2) break;
            if (response.statusCode() / 100 != 5 && response.statusCode() != 429) {
                throw new IOException("Open-Meteo returned HTTP " + response.statusCode());
            }
            if (attempt < attempts) {
                Thread.sleep(properties.getRetryDelayMs() * attempt);
            }
        }
        if (response == null || response.statusCode() / 100 != 2) {
            throw new IOException("Open-Meteo request failed after " + attempts + " attempts");
        }
        JsonNode current = objectMapper.readTree(response.body()).path("current");
        if (!current.hasNonNull("time") || !current.hasNonNull("temperature_2m")
                || !current.hasNonNull("relative_humidity_2m")) {
            throw new IOException("Open-Meteo response is missing current weather fields");
        }
        long eventTime = parseObservationTime(current.path("time").asText());
        return new WeatherEvent(location.getId(), location.getRegion(),
                current.path("temperature_2m").asDouble(),
                current.path("relative_humidity_2m").asDouble(), eventTime);
    }

    private long parseObservationTime(String value) {
        try {
            return Instant.parse(value).toEpochMilli();
        } catch (java.time.format.DateTimeParseException ignored) {
            return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC).toEpochMilli();
        }
    }
}
