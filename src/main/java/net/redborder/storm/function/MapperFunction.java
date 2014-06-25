/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.redborder.storm.metrics.CountMetric;
import org.codehaus.jackson.map.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * Make a java.util.Map from the json string.
 *
 * @author andresgomez
 */
public class MapperFunction extends BaseFunction {

    ObjectMapper _mapper;
    CountMetric _metric;
    String _metricName;

    public MapperFunction(String metric){
        _metricName = metric;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
         _mapper = new ObjectMapper();
        _metric = context.registerMetric("throughput_" + _metricName,  new CountMetric(), 50);
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String jsonEvent = (String) tuple.getValue(0);
        if (jsonEvent != null && jsonEvent.length() > 0) {
            Map<String, Object> event;
            try {
                event = _mapper.readValue(jsonEvent, Map.class);
                _metric.incrEvent();
                collector.emit(new Values(event));
            } catch (IOException | NullPointerException ex) {
                Logger.getLogger(MapperFunction.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class \n"
                        + " JSON tuple: " + jsonEvent, ex);
            }

        }
    }

}
