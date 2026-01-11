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
}
