package net.redborder.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author andresgomez
 */
public class FacebookUserBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_facebook", "topic"));
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
            String http_host = event.get("http_host").toString();
            if (http_host.contains("akamaihd.net") || http_host.contains("ak.fbcdn.net")) {
                String url = event.get("http_url").toString();
                int start = url.indexOf("_") + 1;
                int end = url.indexOf("_", start);
                event.put("social_user",
                        "http://www.facebook.com/profile.php?id=" + url.substring(start, end));
            }
        }

        _collector.emit(new Values(event, topic));
    }

}
