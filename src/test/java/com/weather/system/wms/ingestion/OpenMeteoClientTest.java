package com.weather.system.wms.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OpenMeteoClientTest {
    @Test
    void bindsConfiguredLocation() {
        OpenMeteoProperties properties = new OpenMeteoProperties();
        OpenMeteoProperties.Location location = new OpenMeteoProperties.Location();
        location.setId("bangalore");
        location.setRegion("Bangalore");
        location.setLatitude(12.9716);
        location.setLongitude(77.5946);
        properties.setEndpoint("https://api.open-meteo.com/v1/forecast");
        properties.getLocations().add(location);

        assertEquals("bangalore", properties.getLocations().get(0).getId());
        assertEquals(12.9716, properties.getLocations().get(0).getLatitude());
    }
}
