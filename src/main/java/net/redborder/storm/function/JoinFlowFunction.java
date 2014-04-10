/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class JoinFlowFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        List<Object> data = (List<Object>) tuple.getValues();
        
        Map<String, Object> flow = (Map<String, Object>) data.get(0);
        data.remove(flow);       
                
        for(Object value : data){
            Map<String, Object> valueMap = (Map<String, Object>) value;
            if(!valueMap.isEmpty()){
                flow.putAll(valueMap);
            }
        }

        collector.emit(new Values(flow));
    }

}
