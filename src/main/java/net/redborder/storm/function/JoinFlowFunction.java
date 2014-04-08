/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
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
        Map<String, Object> flow = (Map<String, Object>) tuple.getValueByField("flows");
        String sta_mac_address_latlong = tuple.getStringByField("sta_mac_address_latlong");
        
        if (!sta_mac_address_latlong.equals("")) {
            flow.put("sta_mac_address_latlong", sta_mac_address_latlong);
        }
            
        String client_mac_vendor = tuple.getStringByField("client_mac_vendor");
        
        if (!client_mac_vendor.equals("")) {
            flow.put("client_mac_vendor", client_mac_vendor);
        }
        
        collector.emit(new Values(flow));
    }
    
}
