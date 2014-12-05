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
 * @author andresgomez
 */
public class MergeMapsFunction extends BaseFunction {

    Boolean _hash_mac = false;
    List<String> _fields;

    public MergeMapsFunction(List<String> fields) {
        _fields = fields;
    }

    public MergeMapsFunction(){
        _fields=null;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        _hash_mac = (Boolean) conf.get("hash_mac");
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> finalMap = new HashMap<>();
        Map<String, Object> flow = (Map<String, Object>) tuple.get(0);

        if (_fields == null) {
            List<Object> data = new LinkedList<>(tuple.getValues());
            data.remove(flow);
            for (Object value : data) {
                Map<String, Object> valueMap = (Map<String, Object>) value;
                if (!valueMap.isEmpty()) {
                    finalMap.putAll(valueMap);
                }
            }
        } else {
            for (String field : _fields) {
                Map<String, Object> valueMap = (Map<String, Object>) tuple.getValueByField(field);
                if (!valueMap.isEmpty()) {
                    finalMap.putAll(valueMap);
                }
            }
        }

        finalMap.putAll(flow);

        if (_hash_mac) {
            String mac = (String) finalMap.get("client_mac");
            if (mac != null)
                finalMap.put("client_mac", mac.hashCode());
        }
        collector.emit(new Values(finalMap));
    }

}
