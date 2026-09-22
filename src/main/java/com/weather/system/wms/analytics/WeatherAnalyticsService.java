package com.weather.system.wms.analytics;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
public class WeatherAnalyticsService {
    private final String url;
    private final String user;
    private final String password;

    public WeatherAnalyticsService(@Value("${clickhouse.host}") String host,
                                   @Value("${clickhouse.port}") String port,
                                   @Value("${clickhouse.database}") String database,
                                   @Value("${clickhouse.user}") String user,
                                   @Value("${clickhouse.password}") String password) {
        this.url = "jdbc:clickhouse://" + host + ":" + port + "/" + database + "?compress=0";
        this.user = user;
        this.password = password;
    }

    public Map<String, Object> dashboard() throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("summary", one(connection, """
                    SELECT count(), uniqExact(region), avg(temperature), avg(humidity),
                           min(event_time), max(event_time)
                    FROM weather.weather_events"""));
            result.put("latest", rows(connection, """
                    SELECT region, device_id, latest_event_time, temperature, humidity
                    FROM weather.latest_weather_by_region ORDER BY region"""));
            result.put("hourly", rows(connection, """
                    SELECT region, hour, event_count, round(avg_temperature, 2) AS avg_temperature,
                           round(avg_humidity, 2) AS avg_humidity
                    FROM weather.hourly_weather_analytics
                    WHERE hour >= now() - INTERVAL 24 HOUR
                    ORDER BY hour DESC, region LIMIT 100"""));
            result.put("recent", rows(connection, """
                    SELECT device_id, region, event_time, temperature, humidity
                    FROM weather.weather_events ORDER BY event_time DESC LIMIT 20"""));
            return result;
        }
    }

    private Map<String, Object> one(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            ResultSetMetaData metadata = result.getMetaData();
            Map<String, Object> row = new LinkedHashMap<>();
            if (result.next()) for (int i = 1; i <= metadata.getColumnCount(); i++) {
                row.put(metadata.getColumnLabel(i), result.getObject(i));
            }
            return row;
        }
    }

    private List<Map<String, Object>> rows(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            ResultSetMetaData metadata = result.getMetaData();
            List<Map<String, Object>> rows = new ArrayList<>();
            while (result.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    Object value = result.getObject(i);
                    row.put(metadata.getColumnLabel(i), value instanceof Timestamp ? value.toString() : value);
                }
                rows.add(row);
            }
            return rows;
        }
    }
}
