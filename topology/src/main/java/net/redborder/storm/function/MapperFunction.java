/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import net.redborder.metrics.CountMetric;
import org.codehaus.jackson.map.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>This function converts a JSON events to JAVA Map.</p>
 *
 * @author Andres Gomez
 */
public class MapperFunction extends BaseFunction {

    /**
     * This object is responsible for making the conversion.
     */
    ObjectMapper _mapper;
    /**
     * This metrics is used to calculate the throughput.
     */
    CountMetric _metric;
    /**
     * Name of throughput metric.
     */
    String _metricName;

    boolean firstTime = true;

    /**
     * <p>This function converts a JSON events to JAVA Map.</p>
     *
     * @param metric The name of the metric.
     */
    public MapperFunction(String metric) {
        _metricName = metric;
    }

    /**
     * <p>Initialize ObjectMapper. Register throughput metrics.</p>
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _mapper = new ObjectMapper();
        _metric = context.registerMetric("throughput_" + _metricName, new CountMetric(), 50);
    }

    /**
     * <p>This function converts a JSON events to JAVA Map.</p>
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String jsonEvent = tuple.getString(0);
        if (jsonEvent != null && jsonEvent.length() > 0) {
            Map<String, Object> event = null;
            try {
                event = _mapper.readValue(jsonEvent, Map.class);
                _metric.incrEvent();
            } catch (IOException | NullPointerException ex) {
                Logger.getLogger(MapperFunction.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class [ " + _metricName + " ] \n"
                        + " JSON tuple: " + jsonEvent, ex);
            }
            if (event != null)
                collector.emit(new Values(event));
        }
    }

}
