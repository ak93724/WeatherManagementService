package com.weather.system.wms.processor.consumer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WeatherEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(WeatherEventConsumer.class);

    @Value("${redpanda.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${redpanda.topic.weather-events}")
    private String topic;

    @Value("${redpanda.consumer-group}")
    private String consumerGroup;

    public KafkaSource<String> source() {
        logger.info("Creating Kafka source with bootstrap servers: {}, topic: {}, group: {}",
                   bootstrapServers, topic, consumerGroup);

        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
