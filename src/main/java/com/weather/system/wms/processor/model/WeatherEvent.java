package com.weather.system.wms.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WeatherEvent {
    private String deviceId;

    private String region;

    private double temperature;

    private double humidity;

    private long eventTime;

    public void validate() {
        if (deviceId == null || deviceId.isBlank()) throw new IllegalArgumentException("deviceId is required");
        if (region == null || region.isBlank()) throw new IllegalArgumentException("region is required");
        if (!Double.isFinite(temperature) || !Double.isFinite(humidity)) {
            throw new IllegalArgumentException("temperature and humidity must be finite");
        }
        if (eventTime <= 0) throw new IllegalArgumentException("eventTime must be positive");
    }
}
