package com.weather.system.wms.processor.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlinkConfig {

    @Bean
    public StreamExecutionEnvironment flinkExecutionEnvironment() {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1); // learning mode
        return env;
    }
}
