/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.redborder.storm.trident.function;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class GetMSEdata extends BaseFunction{

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String,Object> mseEvent = (Map<String,Object>) tuple.get(0);
        Map<String, Object> location = (Map<String,Object>) mseEvent.get("location");
        String macAddress = location.get("macAddress").toString();
        
        Map<String,Object> geoCoordinate = (Map<String,Object>) location.get("geoCoordinate");
        System.out.println("GET: " + geoCoordinate);
        collector.emit(new Values(macAddress,geoCoordinate));
    }
    
}
