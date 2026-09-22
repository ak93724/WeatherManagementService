package com.weather.system.wms.processor.sink;

import com.weather.system.wms.processor.model.WeatherAggregation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

@Component
public class ClickHouseAggregationSink extends ClickHouseWeatherSink<WeatherAggregation> {
    public ClickHouseAggregationSink(@Value("${clickhouse.host}") String host,
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
        return "INSERT INTO weather.weather_aggregates " +
                "(region, window_start, window_end, event_count, min_temperature, max_temperature, " +
                "avg_temperature, min_humidity, max_humidity, avg_humidity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    protected void bind(PreparedStatement statement, WeatherAggregation aggregate) throws Exception {
        statement.setString(1, aggregate.getRegion());
        statement.setTimestamp(2, new Timestamp(aggregate.getWindowStart()));
        statement.setTimestamp(3, new Timestamp(aggregate.getWindowEnd()));
        statement.setLong(4, aggregate.getEventCount());
        statement.setDouble(5, aggregate.getMinTemperature());
        statement.setDouble(6, aggregate.getMaxTemperature());
        statement.setDouble(7, aggregate.getAvgTemperature());
        statement.setDouble(8, aggregate.getMinHumidity());
        statement.setDouble(9, aggregate.getMaxHumidity());
        statement.setDouble(10, aggregate.getAvgHumidity());
    }
}
