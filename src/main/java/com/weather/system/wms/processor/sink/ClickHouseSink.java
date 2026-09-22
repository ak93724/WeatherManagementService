package com.weather.system.wms.processor.sink;

import com.weather.system.wms.processor.model.WeatherEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

@Component
public class ClickHouseSink extends ClickHouseWeatherSink<WeatherEvent> {
    public ClickHouseSink(@Value("${clickhouse.host}") String host,
                          @Value("${clickhouse.port}") String port,
                          @Value("${clickhouse.database}") String database,
                          @Value("${clickhouse.user}") String user,
                          @Value("${clickhouse.password}") String password,
                          @Value("${clickhouse.batch-size}") int batchSize,
                          @Value("${clickhouse.flush-interval-ms}") long flushIntervalMs) {
        super(host, port, database, user, password, batchSize, flushIntervalMs);
    }

    @Override
    protected String insertSql() {
        return "INSERT INTO weather.weather_events " +
                "(device_id, region, temperature, humidity, event_time) VALUES (?, ?, ?, ?, ?)";
    }

    @Override
    protected void bind(PreparedStatement statement, WeatherEvent event) throws Exception {
        statement.setString(1, event.getDeviceId());
        statement.setString(2, event.getRegion());
        statement.setDouble(3, event.getTemperature());
        statement.setDouble(4, event.getHumidity());
        statement.setTimestamp(5, new Timestamp(event.getEventTime()));
    }
}
