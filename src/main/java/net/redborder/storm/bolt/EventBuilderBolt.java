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
import net.redborder.storm.util.RBEventType;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author andresgomez
 */
public class EventBuilderBolt extends BaseRichBolt {

    OutputCollector _collector;
    int _topic;

    public  EventBuilderBolt(int topic) {
        _topic = topic;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (_topic == RBEventType.EVENT) {
            declarer.declare(new Fields("event", "topic"));
        } else if (_topic == RBEventType.FLOW) {
            declarer.declare(new Fields("flow", "topic"));
        } else if (_topic == RBEventType.MONITOR) {
            declarer.declare(new Fields("monitor", "topic"));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String jsonEvent = tuple.getValue(0).toString();
        if (jsonEvent != null && jsonEvent.length() > 0) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> event = null;
            try {
                event = mapper.readValue(jsonEvent, Map.class);
            } catch (IOException ex) {
                Logger.getLogger(EventBuilderBolt.class.getName()).log(Level.SEVERE, null, ex);
            }
            if (event != null) {
                _collector.emit(new Values(event, _topic));
                _collector.ack(tuple);
            }
        }
    }

}
