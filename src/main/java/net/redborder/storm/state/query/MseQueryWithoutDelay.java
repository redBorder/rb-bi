/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.trident.state.query;

import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class MseQueryWithoutDelay extends BaseQueryFunction<MapState<Map<String, Object>>, Map<String, Object>> {

    String _log;
    String _field;
    String _key;

    public MseQueryWithoutDelay(String key, String field) {
        _log = "";
        _field = field;
        _key = key;

    }

    public MseQueryWithoutDelay(String log, String key, String field) {
        this(key, field);
        _log = log;
    }

    @Override
    public List<Map<String, Object>> batchRetrieve(MapState<Map<String, Object>> state, List<TridentTuple> tuples) {
        int tupleSize = tuples.size();
        List<Map<String, Object>> mseFlows = new ArrayList<Map<String, Object>>();
        List<List<Object>> keys = Lists.newArrayList();
        for (TridentTuple t : tuples) {
            List<Object> l = Lists.newArrayList();
            l.add(t.getValueByField(_key));
            keys.add(l);
        }

        List<Map<String, Object>> memcached = state.multiGet(keys);
        //System.out.println("tupleSize " + tupleSize + " MAP: " + memcached.toString());
        if (memcached != null && !memcached.isEmpty()) {
            for (Map<String, Object> event : memcached) {
                if (event == null) {
                    mseFlows.add(null);
                    tupleSize--;
                } else {
                    mseFlows.add(event);
                    tupleSize--;
                }
            }
            if (tupleSize != 0) {
                for (int i = 0; i < tupleSize; i++) {
                    mseFlows.add(null);
                }
            }
        } else {
            for (int i = 0; i < tuples.size(); i++) {
                mseFlows.add(null);
            }
        }

        return mseFlows;
    }

    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValueByField(_field);
        if (result == null) {
            collector.emit(new Values(event));
        } else {
            event.put("mse_location", result);
            collector.emit(new Values(event));
        }
    }

}
