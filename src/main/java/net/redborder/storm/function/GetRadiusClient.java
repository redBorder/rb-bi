/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class GetRadiusClient extends BaseFunction {
    
    boolean debug;
    
    public GetRadiusClient(boolean debug){
        this.debug=debug;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> radiusData = (Map<String, Object>) tuple.getValue(0);

        String clientMac = radiusData.get("Calling-Station-Id").toString();
        clientMac = clientMac.replace("-", ":");
        
        Map<String, Object> radiusMap = new HashMap<>();
        
        radiusMap.put("client_mac", clientMac);
        
        if(debug){
            System.out.println(GetRadiusClient.class +" - Radius client to query: " + clientMac);
        }
        
        collector.emit(new Values(radiusMap));
    }

}
