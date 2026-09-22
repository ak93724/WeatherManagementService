package com.weather.system.wms.processor.job;

import com.weather.system.wms.processor.consumer.WeatherEventConsumer;
import com.weather.system.wms.processor.model.WeatherEvent;
import com.weather.system.wms.processor.parser.WeatherEventParser;
import com.weather.system.wms.processor.sink.ClickHouseSink;
import com.weather.system.wms.processor.sink.ClickHouseAggregationSink;
import com.weather.system.wms.processor.aggregation.WeatherAggregationFunction;
import com.weather.system.wms.processor.WeatherEventTimestampAssigner;
import jakarta.annotation.PreDestroy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.time.Duration;

@Component
@ConditionalOnProperty(name = "weather.streaming.enabled", havingValue = "true", matchIfMissing = true)
public class WeatherStreamingJob implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(WeatherStreamingJob.class);

    private final StreamExecutionEnvironment env;
    private final WeatherEventConsumer consumer;
    private final ClickHouseSink clickHouseSink;
    private final ClickHouseAggregationSink aggregationSink;

    public WeatherStreamingJob(StreamExecutionEnvironment env,
                               WeatherEventConsumer consumer,
                               ClickHouseSink clickHouseSink,
                               ClickHouseAggregationSink aggregationSink) {
        this.env = env;
        this.consumer = consumer;
        this.clickHouseSink = clickHouseSink;
        this.aggregationSink = aggregationSink;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("Starting Weather Streaming Job");

        DataStream<WeatherEvent> weatherEvents =
                env.fromSource(
                        consumer.source(),
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withIdleness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new WeatherEventTimestampAssigner()),
                        "Weather Events Source")
                        .map(new WeatherEventParser());

        weatherEvents.addSink(clickHouseSink);
        weatherEvents
                .keyBy(WeatherEvent::getRegion)
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .aggregate(new WeatherAggregationFunction(), new WeatherAggregationFunction.WindowResult())
                .addSink(aggregationSink);

        logger.info("Executing Weather → ClickHouse Job asynchronously");
        env.executeAsync("Weather → ClickHouse Job");
    }

    @PreDestroy
    public void stop() {
        // Flink handles shutdown via JVM lifecycle
        // This hook is useful later for cleanup
    }
}
