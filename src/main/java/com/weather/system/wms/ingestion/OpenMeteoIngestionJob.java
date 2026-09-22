package com.weather.system.wms.ingestion;

import com.weather.system.wms.api.WeatherEventProducer;
import com.weather.system.wms.processor.model.WeatherEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnProperty(name = "weather.ingestion.enabled", havingValue = "true")
public class OpenMeteoIngestionJob {
    private static final Logger logger = LoggerFactory.getLogger(OpenMeteoIngestionJob.class);
    private final OpenMeteoProperties properties;
    private final OpenMeteoClient client;
    private final WeatherEventProducer producer;
    private final Map<String, Long> lastPublishedTimes = new ConcurrentHashMap<>();

    public OpenMeteoIngestionJob(OpenMeteoProperties properties, OpenMeteoClient client,
                                 WeatherEventProducer producer) {
        this.properties = properties;
        this.client = client;
        this.producer = producer;
    }

    @Scheduled(fixedDelayString = "${weather.ingestion.interval-ms:60000}",
            initialDelayString = "${weather.ingestion.initial-delay-ms:5000}")
    public void poll() {
        if (properties.getLocations().isEmpty()) {
            logger.warn("Open-Meteo ingestion is enabled but no locations are configured");
            return;
        }
        for (OpenMeteoProperties.Location location : properties.getLocations()) {
            try {
                WeatherEvent event = client.fetch(location);
                Long lastTimestamp = lastPublishedTimes.get(location.getId());
                if (lastTimestamp != null && lastTimestamp >= event.getEventTime()) {
                    logger.debug("Skipping duplicate Open-Meteo observation for {}", location.getId());
                    continue;
                }
                producer.publish(event);
                lastPublishedTimes.put(location.getId(), event.getEventTime());
                logger.info("Published Open-Meteo observation for {} ({}) at {}",
                        location.getId(), location.getRegion(), event.getEventTime());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                logger.error("Open-Meteo polling interrupted for {}", location.getId(), exception);
                return;
            } catch (RuntimeException | java.io.IOException exception) {
                logger.error("Failed to ingest Open-Meteo observation for {}", location.getId(), exception);
            }
        }
    }
}
