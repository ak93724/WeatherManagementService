package com.weather.system.wms.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class WeatherEventTimestampAssigner implements SerializableTimestampAssigner<String> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public long extractTimestamp(String jsonString, long recordTimestamp) {
        try {
            JsonNode node = mapper.readTree(jsonString);
            JsonNode eventTimeNode = node.get("eventTime");

            if (eventTimeNode == null) {
                return recordTimestamp;
            }

            // Handle both string and numeric formats
            if (eventTimeNode.isTextual()) {
                // Parse ISO string format (e.g., "2025-12-26T01:30:00Z")
                return java.time.Instant.parse(eventTimeNode.asText()).toEpochMilli();
            } else if (eventTimeNode.isNumber()) {
                // Handle numeric timestamp
                return eventTimeNode.asLong();
            } else {
                return recordTimestamp;
            }
        } catch (Exception e) {
            return recordTimestamp; // fallback to record timestamp
        }
    }
}
