CREATE DATABASE IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.weather_events
(
    device_id String,
    region String,
    temperature Float64,
    humidity Float64,
    event_time DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (region, event_time, device_id);

CREATE TABLE IF NOT EXISTS weather.weather_aggregates
(
    region String,
    window_start DateTime64(3),
    window_end DateTime64(3),
    event_count UInt64,
    min_temperature Float64,
    max_temperature Float64,
    avg_temperature Float64,
    min_humidity Float64,
    max_humidity Float64,
    avg_humidity Float64
)
ENGINE = MergeTree
ORDER BY (region, window_start);

CREATE VIEW IF NOT EXISTS weather.hourly_weather_analytics AS
SELECT
    region,
    toStartOfHour(event_time) AS hour,
    count() AS event_count,
    min(temperature) AS min_temperature,
    max(temperature) AS max_temperature,
    avg(temperature) AS avg_temperature,
    min(humidity) AS min_humidity,
    max(humidity) AS max_humidity,
    avg(humidity) AS avg_humidity
FROM weather.weather_events
GROUP BY region, hour;

CREATE VIEW IF NOT EXISTS weather.daily_weather_analytics AS
SELECT
    region,
    toDate(event_time) AS day,
    count() AS event_count,
    min(temperature) AS min_temperature,
    max(temperature) AS max_temperature,
    avg(temperature) AS avg_temperature,
    min(humidity) AS min_humidity,
    max(humidity) AS max_humidity,
    avg(humidity) AS avg_humidity
FROM weather.weather_events
GROUP BY region, day;

CREATE VIEW IF NOT EXISTS weather.latest_weather_by_region AS
SELECT
    region,
    argMax(device_id, event_time) AS device_id,
    max(event_time) AS latest_event_time,
    argMax(temperature, event_time) AS temperature,
    argMax(humidity, event_time) AS humidity
FROM weather.weather_events
GROUP BY region;
