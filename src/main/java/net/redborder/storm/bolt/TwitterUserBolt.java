/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.bolt;

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
public class TwitterUserBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_twitter", "topic"));
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
            if (event.get("http_host").toString().contains("api.twitter.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("screen_name")) {
                    int start = url.indexOf("screen_name=") + "screen_name=".length();
                    int end = url.indexOf("&", start);
                    event.put("social_user", "http://www.twitter.com/" + url.substring(start, end));
                }
            } else if (event.get("http_hosts").toString().contains("twitter.com")){
                String url = event.get("http_url").toString();
                if (url.contains("status")){
                    int end = url.indexOf("/");
                    event.put("social_user", "http://www.twitter.com/" + url.substring(0, end));
                }
            }
        }

        _collector.emit(new Values(event, topic));
    }
}
