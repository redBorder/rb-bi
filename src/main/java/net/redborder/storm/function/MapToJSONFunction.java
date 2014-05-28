/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.Map;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class MapToJSONFunction extends BaseFunction {

    ObjectMapper _mapper;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String eventJSON = "";
        eventJSON = _mapper.writeValueAsString((Map<String, Object>) tuple.getValue(0));
        collector.emit(new Values(eventJSON));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _mapper = JsonFactory.create();
    }

}
