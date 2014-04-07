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
 * Get the facebook likes and shares of the http_url.
 *
 * @author andresgomez
 */
public class FacebookLikeShareFuction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsValue("http_host")) {
            if (event.get("http_host").toString().contains("www.facebook.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("plugins/like.php")) {
                    int start = url.indexOf("href=") + "href=".length();
                    int end = url.indexOf("&", start);

                    event.put("facebook_like",
                            url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));

                } else if (url.contains("/plugins/share_button")) {
                    int start = url.indexOf("href=") + "href=".length();
                    int end = url.indexOf("&", start);

                    event.put("facebook_share",
                            url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
                }
            }
        }

        collector.emit(new Values(event));
    }
}
