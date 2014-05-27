/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
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

    ObjectMapper mapper;
    String jsonEvent;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        mapper = JsonFactory.create();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        jsonEvent = tuple.getString(0);
        if (jsonEvent != null && jsonEvent.length() > 0) {
            try {
                Map<String, Object> event = mapper.readValue(jsonEvent, Map.class);
                collector.emit(new Values(event));
            } catch (NullPointerException ex) {
                Logger.getLogger(MapperFunction.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class: " + jsonEvent, ex);
            }
        }
    }

}
