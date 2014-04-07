/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.trident.function;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Get the facebook user of the http_url.
 *
 * @author andresgomez
 */
public class FacebookUserFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsValue("http_host")) {
            String http_host = event.get("http_host").toString();
            if (http_host.contains("akamaihd.net") || http_host.contains("ak.fbcdn.net")) {
                String url = event.get("http_url").toString();
                int start = url.indexOf("_") + 1;
                int end = url.indexOf("_", start);
                event.put("social_user",
                        "http://www.facebook.com/profile.php?id=" + url.substring(start, end));
            }
        }

        collector.emit(new Values(event));
    }

}
