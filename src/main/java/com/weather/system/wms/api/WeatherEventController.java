package com.weather.system.wms.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.weather.system.wms.processor.model.WeatherEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/weather/events")
public class WeatherEventController {
    private final WeatherEventProducer producer;

    public WeatherEventController(WeatherEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Void> publish(@RequestBody WeatherEvent event) throws JsonProcessingException {
        producer.publish(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<String> invalidEvent(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(exception.getMessage());
    }
}
