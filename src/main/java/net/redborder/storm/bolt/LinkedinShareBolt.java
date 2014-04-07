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
public class LinkedinShareBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("share_linkedin", "topic"));
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

        _collector.emit(new Values(event, topic));
    }
}
