/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.state.riak;

import backtype.storm.topology.ReportedFailedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

/**
 * @author andresgomez
 */
public class RiakUpdater extends BaseStateUpdater<MapState<Map<String, Object>>> {

    String _key;
    String _value;
    String _generalKey;
    private boolean _debug;

    public RiakUpdater(String key, String value) {
        _key = key;
        _value = value;
        _generalKey = "";
    }

    public RiakUpdater(String key, String value, String generalKey) {
        this(key, value);
        _generalKey = "rbbi:" + generalKey + ":";
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _debug = (boolean) conf.get("rbDebug");
    }

    @Override
    public void updateState(MapState<Map<String, Object>> state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Map<String, Object>> events = new ArrayList<>();
        List<List<Object>> keys = new ArrayList<>();
        for (TridentTuple t : tuples) {
            if (t != null) {
                List<Object> l = new ArrayList<>();
                l.add(_generalKey + t.getValueByField(_key));
                keys.add(l);
                if (t.getValueByField(_value) != null)
                    events.add((Map<String, Object>) t.getValueByField(_value));

                if (_debug) {
                    System.out.println("SAVED TO RIAK KEY: " + _generalKey + t.getValueByField(_key)
                            + " VALUE: " + t.getValueByField(_value));
                }
            }
        }

        try {
            if (!events.isEmpty())
                state.multiPut(keys, events);
        } catch (ReportedFailedException e) {
            Logger.getLogger(RiakUpdater.class.getName()).log(Level.WARNING, null, e);
        }
    }

}
