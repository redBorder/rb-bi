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
public class HttpSocialMediaFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsValue("http_url")) {
            String host = event.get("http_host").toString();
            String url = event.get("http_url").toString();
            String extension = url.substring(url.length() - 5, url.length());

            if (extension.contains(".jpg") || extension.contains(".gif")
                    || extension.contains(".png") || extension.contains(".mp4")
                    || extension.contains(".avi")
                    || extension.contains(".mp3")) {

                event.put("http_social_media",
                        "http://" + host + url);

            }
        }
        collector.emit(new Values(event));
    }
}
