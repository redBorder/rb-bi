/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.state.gridgain;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author andresgomez
 */
public class GridGainUpdater extends BaseStateUpdater<MapState<Map<String, Map<String, Object>>>> {

    String _key;
    String _value;
    String _generalKey;
    private boolean _debug;


    public GridGainUpdater(String key, String value) {
        _key = key;
        _value = value;
        _generalKey = "";
    }

    public GridGainUpdater(String key, String value, String generalKey) {
        this(key, value);
        _generalKey = "rbbi:" + generalKey + ":";
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _debug = (boolean) conf.get("rbDebug");
    }

    @Override
    public void updateState(MapState<Map<String, Map<String, Object>>> state, List<TridentTuple> tuples, TridentCollector collector) {
        Map<String, Map<String, Object>> keyValue = new HashMap<>();
        List<Map<String, Map<String, Object>>> events = new ArrayList<>();
        List<List<Object>> keys = new ArrayList<>();

        for (TridentTuple t : tuples) {
            if (t != null) {
                List<Object> l = new ArrayList<>();
                l.add(_generalKey + t.getValueByField(_key));
                keys.add(l);
                keyValue.put(_generalKey + t.getValueByField(_key), (Map<String, Object>) t.getValueByField(_value));

                if (_debug) {
                    System.out.println("SAVED TO GRIDGAIN, KEY: " + _generalKey + t.getValueByField(_key)
                            + " VALUE: " + t.getValueByField(_value));
                }
            }
        }

        events.add(keyValue);

        try {
            state.multiPut(keys, events);
        } catch (Exception e) {
            Logger.getLogger(GridGainUpdater.class.getName()).log(Level.SEVERE, null, e);
            e.printStackTrace();
        }
    }

}
