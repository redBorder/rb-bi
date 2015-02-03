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
 * <p>This function converts a JAVA Map to JSON events.</p>
 * @author Andres Gomez
 */
public class MapToJSONFunction extends BaseFunction {

    /**
     * This object is responsible for making the conversion.
     */
    ObjectMapper _mapper;

    CountMetric _metric;

    String _topic;

    public MapToJSONFunction(String topic){
        _topic=topic;
    }


    /**
     * <p>This function converts a JAVA Map to JSON events.</p>
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String eventJSON = "";
        try {
            eventJSON = _mapper.writeValueAsString((Map<String, Object>) tuple.getValue(0));
        } catch (IOException ex) {
            Logger.getLogger(MapToJSONFunction.class.getName()).log(Level.SEVERE, null, ex);
        }
        _metric.incrEvent();

        collector.emit(new Values(eventJSON));
    }

    /**
     * <p>Initialize ObjectMapper. Register throughput metrics.</p>
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _mapper = new ObjectMapper();
        _metric = context.registerMetric("throughput_" + _topic+"_post", new CountMetric(), 50);
    }

}
