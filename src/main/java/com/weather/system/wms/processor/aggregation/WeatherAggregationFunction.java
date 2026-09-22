package com.weather.system.wms.processor.aggregation;

import com.weather.system.wms.processor.model.WeatherAggregation;
import com.weather.system.wms.processor.model.WeatherEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WeatherAggregationFunction
        implements AggregateFunction<WeatherEvent, WeatherAggregationFunction.Accumulator, WeatherAggregation> {

    public static class Accumulator {
        long count;
        double temperatureSum;
        double minTemperature = Double.MAX_VALUE;
        double maxTemperature = -Double.MAX_VALUE;
        double humiditySum;
        double minHumidity = Double.MAX_VALUE;
        double maxHumidity = -Double.MAX_VALUE;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(WeatherEvent event, Accumulator accumulator) {
        accumulator.count++;
        accumulator.temperatureSum += event.getTemperature();
        accumulator.minTemperature = Math.min(accumulator.minTemperature, event.getTemperature());
        accumulator.maxTemperature = Math.max(accumulator.maxTemperature, event.getTemperature());
        accumulator.humiditySum += event.getHumidity();
        accumulator.minHumidity = Math.min(accumulator.minHumidity, event.getHumidity());
        accumulator.maxHumidity = Math.max(accumulator.maxHumidity, event.getHumidity());
        return accumulator;
    }

    @Override
    public WeatherAggregation getResult(Accumulator accumulator) {
        return new WeatherAggregation(null, 0, 0, accumulator.count,
                accumulator.minTemperature, accumulator.maxTemperature,
                accumulator.temperatureSum / accumulator.count, accumulator.minHumidity,
                accumulator.maxHumidity, accumulator.humiditySum / accumulator.count);
    }

    @Override
    public Accumulator merge(Accumulator first, Accumulator second) {
        first.count += second.count;
        first.temperatureSum += second.temperatureSum;
        first.minTemperature = Math.min(first.minTemperature, second.minTemperature);
        first.maxTemperature = Math.max(first.maxTemperature, second.maxTemperature);
        first.humiditySum += second.humiditySum;
        first.minHumidity = Math.min(first.minHumidity, second.minHumidity);
        first.maxHumidity = Math.max(first.maxHumidity, second.maxHumidity);
        return first;
    }

    public static class WindowResult extends ProcessWindowFunction<WeatherAggregation, WeatherAggregation, String, TimeWindow> {
        @Override
        public void process(String region, Context context, Iterable<WeatherAggregation> values,
                            Collector<WeatherAggregation> out) {
            WeatherAggregation result = values.iterator().next();
            result.setRegion(region);
            result.setWindowStart(context.window().getStart());
            result.setWindowEnd(context.window().getEnd());
            out.collect(result);
        }
    }
}
