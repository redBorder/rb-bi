/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redborder.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/**
 *
 * @author andresgomez
 */
public class YoutubeUserBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_youtube", "topic"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        int topic = (int) tuple.getValueByField("topic");
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
        _collector.emit(new Values(event, topic));
    }

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
