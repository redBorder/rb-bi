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
public class GoogleMapsLocationBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("social_media_event", "topic"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        int topic = (int) tuple.getValueByField("topic");
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
        _collector.emit(new Values(event, topic));
    }

}
