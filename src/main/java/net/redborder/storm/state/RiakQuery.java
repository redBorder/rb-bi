/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.state;

import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class RiakQuery extends BaseQueryFunction<MapState<Map<String, Object>>, Map<String, Object>> {

    String _key;
    String _generalkey;
    boolean debug;

    public RiakQuery(String key, boolean debug) {
        _key = key;
        _generalkey = "";
        this.debug = debug;
    }

    public RiakQuery(String key, String generalKey, boolean debug) {
        this(key, debug);
        _generalkey = "rbbi:" + generalKey + ":";
    }

    @Override
    public List<Map<String, Object>> batchRetrieve(MapState<Map<String, Object>> state, List<TridentTuple> tuples) {
        List<Map<String, Object>> memcachedData = null;
        List<Map<String, Object>> result = new ArrayList<>();
        List<Object> keysToRequest = new ArrayList<>();
        List<String> keysToAppend = new ArrayList<>();

        for (TridentTuple t : tuples) {
            Map<String, Object> flow = (Map<String, Object>) t.getValue(0);
            String key = (String) flow.get(_key);

            if (key != null) {
                keysToAppend.add(_generalkey + key);

                if (!keysToRequest.contains(_generalkey + key)) {
                    keysToRequest.add(_generalkey + key);
                }
            } else {
                keysToAppend.add(null);
            }
        }

        if (debug) {
            System.out.println("BatchSize " + tuples.size()
                    + " RequestedToRiak: " + keysToRequest.size());
        }

        if (!keysToRequest.isEmpty()) {
            List<List<Object>> keysToMemcached = new ArrayList<>();

            for (Object key : keysToRequest) {
                List<Object> l = new ArrayList<>();
                l.add(key);
                keysToMemcached.add(l);
            }

            try {
                memcachedData = state.multiGet(keysToMemcached);
                if (debug) {
                    System.out.println("RiakResponse: " + memcachedData.toString());
                }
            } catch (ReportedFailedException e) {
                Logger.getLogger(RiakQuery.class.getName()).log(Level.WARNING, null, e);
            }
        }

        for (String key : keysToAppend) {
            if (key != null && memcachedData != null
                    && !memcachedData.isEmpty() && keysToRequest.contains(key)) {
                result.add(memcachedData.get(keysToRequest.indexOf(key)));
            } else {
                result.add(null);
            }
        }

        return result;
    }

    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        if (result == null) {
            Map<String, Object> empty = new HashMap<>();
            collector.emit(new Values(empty));
        } else {
            collector.emit(new Values(result));
        }
    }

}
