package com.weather.system.wms.processor.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public abstract class ClickHouseWeatherSink<T> extends RichSinkFunction<T> {
    private final String host;
    private final String port;
    private final String database;
    private final String user;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMs;
    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient List<T> batch;
    private transient long lastFlush;

    protected ClickHouseWeatherSink(String host, String port, String database, String user,
                                    String password, int batchSize, long flushIntervalMs) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    protected abstract String insertSql();
    protected abstract void bind(PreparedStatement statement, T value) throws Exception;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(
                "jdbc:clickhouse://" + host + ":" + port + "/" + database + "?compress=0", user, password);
        statement = connection.prepareStatement(insertSql());
        batch = new ArrayList<>(batchSize);
        lastFlush = System.currentTimeMillis();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        batch.add(value);
        if (batch.size() >= batchSize || System.currentTimeMillis() - lastFlush >= flushIntervalMs) {
            flush();
        }
    }

    private void flush() throws Exception {
        if (batch.isEmpty()) return;
        for (T value : batch) {
            bind(statement, value);
            statement.addBatch();
        }
        statement.executeBatch();
        batch.clear();
        lastFlush = System.currentTimeMillis();
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }
}
