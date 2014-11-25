/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author andresgomez
 */
public class MergeMapsFunction extends BaseFunction {

    Boolean _hash_mac = false;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        _hash_mac = (Boolean) conf.get("hash_mac");
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        List<Object> data = new LinkedList<>(tuple.getValues());
        Map<String, Object> finalMap = new HashMap<>();
               
        Map<String, Object> flow = (Map<String, Object>) data.get(0);
        data.remove(flow);
                
        for(Object value : data){
            Map<String, Object> valueMap = (Map<String, Object>) value;
            if(!valueMap.isEmpty()){
                finalMap.putAll(valueMap);
            }
        }

        if(_hash_mac){
            String mac = (String) flow.get("client_mac");
            if(mac!=null)
                flow.put("client_mac", mac.hashCode());
        }
        
        finalMap.putAll(flow);
        collector.emit(new Values(finalMap));
    }

}
