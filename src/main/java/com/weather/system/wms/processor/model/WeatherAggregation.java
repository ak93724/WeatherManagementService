package com.weather.system.wms.processor.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherAggregation {
    private String region;
    private long windowStart;
    private long windowEnd;
    private long eventCount;
    private double minTemperature;
    private double maxTemperature;
    private double avgTemperature;
    private double minHumidity;
    private double maxHumidity;
    private double avgHumidity;
}
