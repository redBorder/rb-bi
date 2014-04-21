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
import org.codehaus.jackson.map.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Make a java.util.Map from the json string.
 *
 * @author andresgomez
 */
public class MapperFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String jsonEvent = (String) tuple.getValue(0);
        if (jsonEvent != null && jsonEvent.length() > 0) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> event = null;
            try {
                event = mapper.readValue(jsonEvent, Map.class);
            } catch (IOException ex) {
                Logger.getLogger(MapperFunction.class.getName()).log(Level.SEVERE, "Failed reading a JSON tuple", ex);
            }
            if (event != null) {
                collector.emit(new Values(event));
            }
        }
    }

}
