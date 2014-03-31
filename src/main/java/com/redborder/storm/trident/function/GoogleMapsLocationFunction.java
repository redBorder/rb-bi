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
public class GoogleMapsLocationFunction extends BaseFunction{

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsValue("http_url")) {
            String host = event.get("http_host").toString();
            if (host.contains("maps.google.com")) {
                String url = event.get("http_url").toString();

                if (url.contains("/maps/api/staticmap?")) {

                    int start = url.indexOf("center=") + "center=".length();
                    int end = url.indexOf("&", start);
                    event.put("google_maps_location",
                    url.substring(start, end).replace(",", "%2C"));

                }
            }
        }
        collector.emit(new Values(event));    }
    
}
