package com.weather.system.wms.analytics;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.Map;

@RestController
@RequestMapping("/api/analytics")
public class WeatherAnalyticsController {
    private final WeatherAnalyticsService service;

    public WeatherAnalyticsController(WeatherAnalyticsService service) {
        this.service = service;
    }

    @GetMapping("/dashboard")
    public Map<String, Object> dashboard() throws SQLException {
        return service.dashboard();
    }
}
