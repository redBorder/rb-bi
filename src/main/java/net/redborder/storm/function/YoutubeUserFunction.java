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
 * Get youtube user from http_url.
 *
 * @author andresgomez
 */
public class YoutubeUserFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsValue("http_host")) {
            if (event.get("http_host").toString().contains("gdata.youtube.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("/feeds/api/users/")) {
                    int start = url.indexOf("/feeds/api/users/") + "/feeds/api/users/".length();
                    int end1 = url.indexOf("?", start);
                    int end2 = url.indexOf("/", start);
                    int end = compareLess(end1, end2);

                    if (end != 0) {
                        event.put("social_user",
                                "https://www.youtube.com/user/" + url.substring(start, end));
                    }
                }
            }
        }
        collector.emit(new Values(event));
    }

    /**
     * Compare that number is lower
     *
     * @param end1 Number 1
     * @param end2 Numer 2
     * @return The lower number.
     */
    private int compareLess(int end1, int end2) {

        int end = 0;
        if (end1 <= 0) {
            end1 = 10000000;
        }
        if (end2 <= 0) {
            end2 = 10000000;
        }

        if (end1 < end2) {
            end = end1;
        } else if (end1 > end2) {
            end = end2;
        }

        return end;
    }
}
