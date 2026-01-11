package com.weather.system.wms.processor.sink;

import com.weather.system.wms.processor.model.WeatherEvent;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

@Component
public class ClickHouseSink extends RichSinkFunction<WeatherEvent> {

    @Value("${clickhouse.url}")
    private String url;

    @Value("${clickhouse.user}")
    private String user;

    @Value("${clickhouse.password}")
    private String password;

    private transient Connection connection;
    private transient PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(url, user, password);
        statement = connection.prepareStatement(
            "INSERT INTO weather.weather_events (region, temperature, humidity, event_time) VALUES (?, ?, ?, ?)"
        );
    }

    @Override
    public void invoke(WeatherEvent event, Context context) throws Exception {
        statement.setString(1, event.getRegion());
        statement.setDouble(2, event.getTemperature());
        statement.setDouble(3, event.getHumidity());
        statement.setTimestamp(4, new java.sql.Timestamp(event.getEventTime()));
        statement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (statement != null) statement.close();
        if (connection != null) connection.close();
    }
}
