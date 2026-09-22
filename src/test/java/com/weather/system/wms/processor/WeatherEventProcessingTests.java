package com.weather.system.wms.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.system.wms.processor.aggregation.WeatherAggregationFunction;
import com.weather.system.wms.processor.model.WeatherEvent;
import com.weather.system.wms.processor.parser.WeatherEventParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WeatherEventProcessingTests {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializesAndDeserializesWeatherEvent() throws Exception {
        WeatherEvent event = new WeatherEvent("sensor-001", "Bangalore", 28.5, 65.2, 1779091200000L);
        WeatherEvent copy = mapper.readValue(mapper.writeValueAsString(event), WeatherEvent.class);
        assertEquals(event, copy);
    }

    @Test
    void rejectsInvalidWeatherEvent() {
        WeatherEvent event = new WeatherEvent("", "Bangalore", 28.5, 65.2, 1779091200000L);
        assertThrows(IllegalArgumentException.class, event::validate);
    }

    @Test
    void parsesAndValidatesJsonEvent() throws Exception {
        String json = """
                {"deviceId":"sensor-001","region":"Bangalore","temperature":28.5,
                 "humidity":65.2,"eventTime":1779091200000}
                """;
        WeatherEvent event = new WeatherEventParser().map(json);
        assertEquals(1779091200000L, event.getEventTime());
    }

    @Test
    void rejectsInvalidJsonEvent() {
        String json = "{\"deviceId\":\"sensor-001\",\"region\":\"Bangalore\","
                + "\"temperature\":28.5,\"humidity\":65.2,\"eventTime\":0}";
        assertThrows(Exception.class, () -> new WeatherEventParser().map(json));
    }

    @Test
    void extractsEventTimestamp() {
        String json = "{\"eventTime\":1779091200000}";
        assertEquals(1779091200000L,
                new WeatherEventTimestampAssigner().extractTimestamp(json, 0L));
    }

    @Test
    void aggregatesTemperatureAndHumidity() {
        WeatherAggregationFunction function = new WeatherAggregationFunction();
        WeatherAggregationFunction.Accumulator accumulator = function.createAccumulator();
        function.add(new WeatherEvent("a", "Bangalore", 20, 40, 1), accumulator);
        function.add(new WeatherEvent("b", "Bangalore", 30, 60, 2), accumulator);

        var result = function.getResult(accumulator);
        assertEquals(2, result.getEventCount());
        assertEquals(20, result.getMinTemperature());
        assertEquals(30, result.getMaxTemperature());
        assertEquals(25, result.getAvgTemperature());
        assertEquals(40, result.getMinHumidity());
        assertEquals(60, result.getMaxHumidity());
        assertEquals(50, result.getAvgHumidity());
    }
}
