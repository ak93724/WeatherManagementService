# Local verification

The local stack uses Apache Kafka in KRaft mode and ClickHouse. Start it with:

```bash
docker compose up -d kafka kafka-init clickhouse
./mvnw spring-boot:run
```

The `kafka-init` service creates `weather-events` with three partitions and replication factor one.

Publish an event through the API:

```bash
curl -i -X POST http://localhost:8080/api/weather/events \
  -H 'Content-Type: application/json' \
  -d '{"deviceId":"sensor-001","region":"Bangalore","temperature":28.5,"humidity":65.2,"eventTime":1779091200000}'
```

Inspect Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic weather-events --from-beginning --timeout-ms 5000
```

Inspect raw events:

```bash
curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20weather.weather_events%20FORMAT%20Vertical'
```

Send several events with event times in the same one-minute window, wait for the two-minute watermark, and inspect aggregates:

```bash
curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20weather.weather_aggregates%20FORMAT%20Vertical'
```

Analytics views are available for hourly, daily, and latest-per-region queries:

```bash
curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20weather.hourly_weather_analytics%20FORMAT%20Vertical' \
  -u wms:weather
curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20weather.daily_weather_analytics%20FORMAT%20Vertical' \
  -u wms:weather
curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20weather.latest_weather_by_region%20FORMAT%20Vertical' \
  -u wms:weather
```

## Open-Meteo ingestion

Enable the scheduled Open-Meteo source with one location configured by default:

```bash
WEATHER_INGESTION_ENABLED=true WEATHER_STREAMING_ENABLED=true ./mvnw spring-boot:run
```

The scheduler polls Bangalore every 60 seconds, normalizes the response to `WeatherEvent`, and publishes it to `weather-events`. Configure additional locations through `weather.ingestion.locations` or override the endpoint and interval with `OPEN_METEO_ENDPOINT` and `WEATHER_INGESTION_INTERVAL_MS`. Duplicate observations for a location are skipped by event timestamp.
