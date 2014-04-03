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
 * Get the twitter user from the http_url.
 * @author andresgomez
 */
public class TwitterUserFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsValue("http_host")) {
            if (event.get("http_host").toString().contains("api.twitter.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("screen_name")) {
                    int start = url.indexOf("screen_name=") + "screen_name=".length();
                    int end = url.indexOf("&", start);
                    event.put("social_user", "http://www.twitter.com/" + url.substring(start, end));
                }
            } else if (event.get("http_hosts").toString().contains("twitter.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("status")) {
                    int end = url.indexOf("/");
                    event.put("social_user", "http://www.twitter.com/" + url.substring(0, end));
                }
            }
        }

        collector.emit(new Values(event));
    }
}
