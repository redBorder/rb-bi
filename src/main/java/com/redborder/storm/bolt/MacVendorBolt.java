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
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author andresgomez
 */
public class MacVendorBolt extends BaseRichBolt {

    private String _ouiFilePath;

    Map<String, String> _ouiMap;

    OutputCollector _collector;

    public MacVendorBolt(String ouiPath) {
        _ouiFilePath = ouiPath;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("macVendorEvent", "topic"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        _ouiMap = new HashMap<String, String>();

        InputStream in = null;

        _collector = collector;

        try {
            in = new FileInputStream(_ouiFilePath);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MacVendorBolt.class.getName()).log(Level.SEVERE, null, ex);
        }

        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(isr);

        while (true) {
            String line = null;
            try {
                line = br.readLine();

            } catch (IOException ex) {
                Logger.getLogger(MacVendorBolt.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (line == null) {
                break;
            }

            String[] tokens = line.split("\\|");
            _ouiMap.put(tokens[0].substring(2, 8), tokens[1]);

        }
    }

    @Override
    public void execute(Tuple tuple) {
        int topic = (int) tuple.getValueByField("topic");

        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);

        if (event.containsKey("ethsrc") || event.containsKey("ethdst")) {
            String ouiSrc = buildOui(event.get("ethsrc"));
            String ouiDst = buildOui(event.get("ethdst"));

            if (_ouiMap.get(ouiSrc) != null) {
                event.put("mac_vendor_src", _ouiMap.get(ouiSrc));
            }

            if (_ouiMap.get(ouiDst) != null) {
                event.put("mac_vendor_dst", _ouiMap.get(ouiDst));
            }
        }
        _collector.emit(new Values(event, topic));

    }

    private String buildOui(Object object) {
        if (object != null) {
            String mac = object.toString();
            mac = mac.trim().replace("-", "").replace(":", "");
            return mac.substring(0, 6).toUpperCase();
        } else {
            return "";
        }
    }
}
