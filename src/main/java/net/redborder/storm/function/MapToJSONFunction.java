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
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class MapToJSONFunction extends BaseFunction {
    
    boolean debug;
    
    public MapToJSONFunction(boolean debug){
        this.debug=debug;
    }

    ObjectMapper _mapper;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String eventJSON = "";
        try {
            eventJSON = _mapper.writeValueAsString((Map<String, Object>) tuple.getValue(0));
        } catch (IOException ex) {
            Logger.getLogger(MapToJSONFunction.class.getName()).log(Level.SEVERE, null, ex);
        }
        collector.emit(new Values(eventJSON));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _mapper = new ObjectMapper();
    }

}
