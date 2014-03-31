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
public class LinkedinShareFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsValue("http_host")) {
            if (event.get("http_host").toString().contains("www.linkedin.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("share?url=")) {
                    int start = url.indexOf("share?url=") + "share?url=".length();
                    int end = url.indexOf("&", start);

                    if (end < 0) {
                        end = url.length();
                    }

                    event.put("linkedin_share",
                            url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));

                }
            }
        }

        collector.emit(new Values(event));
    }
}
