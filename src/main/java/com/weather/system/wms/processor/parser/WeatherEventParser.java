package com.weather.system.wms.processor.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.system.wms.processor.model.WeatherEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

@Slf4j
public class WeatherEventParser implements MapFunction<String, WeatherEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public WeatherEvent map(String value) throws Exception {
        log.info("Message from Redpanda value is :{}", value);
        WeatherEvent weatherEvent = null;
        try {
            weatherEvent = mapper.readValue(value, WeatherEvent.class);
        } catch (Exception ex) {
            log.error("failed to parse message : {}", value);
        }
        return weatherEvent;
    }
}
