/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.state.gridgain;

import backtype.storm.tuple.Values;
import net.redborder.storm.util.logger.RbLogger;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author andresgomez
 */
public class GridGainQuery extends BaseQueryFunction<MapState<Map<String, Map<String, Object>>>, Map<String, Object>> {

    String _key;
    String _generalkey;
    private boolean _debug;
    Logger logger;

    public GridGainQuery(String key) {
        _key = key;
        _generalkey = "";
    }

    public GridGainQuery(String key, String generalKey) {
        _generalkey = "rbbi:" + generalKey + ":";
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _debug = (boolean) conf.get("rbDebug");
        logger = RbLogger.getLogger(GridGainQuery.class.getName());
    }

    @Override
    public List<Map<String, Object>> batchRetrieve(MapState<Map<String, Map<String, Object>>> state, List<TridentTuple> tuples) {
        List<Map<String, Map<String, Object>>> gridGainData = null;
        Map<String, Map<String, Object>> queryData = null;
        List<Map<String, Object>> result = new ArrayList<>();
        List<Object> keysToRequest = new ArrayList<>();
        List<String> keysToAppend = new ArrayList<>();


        for (TridentTuple t : tuples) {
                Map<String, Object> flow = (Map<String, Object>) t.getValue(0);
            if(flow!=null) {
                String key = (String) flow.get(_key);

                if (key != null) {
                    keysToAppend.add(_generalkey + key);

                    if (!keysToRequest.contains(_generalkey + key)) {
                        keysToRequest.add(_generalkey + key);
                    }
                } else {
                    keysToAppend.add(null);
                }
            }else{
                keysToAppend.add(null);
            }
        }

        if (_debug) {
            System.out.println("BatchSize " + tuples.size()
                    + " RequestedToGridGain: " + keysToRequest.size());
        }

        if (!keysToRequest.isEmpty()) {
            List<List<Object>> keysToGridGain = new ArrayList<>();

            for (Object key : keysToRequest) {
                List<Object> l = new ArrayList<>();
                l.add(key);
                keysToGridGain.add(l);
            }

            try {
                logger.severe("Calling gridgain multiget");
                gridGainData = state.multiGet(keysToGridGain);
                logger.severe("Back from gridgain multiget");
                if (gridGainData != null) {
                    queryData = gridGainData.get(0);

                    if (_debug) {
                        System.out.println("GridGainResponse: " + gridGainData.toString());
                    }
                }
            } catch (Exception e) {
                Logger.getLogger(GridGainQuery.class.getName()).log(Level.SEVERE, null, e);
                e.printStackTrace();
            }
        }

        for (String key : keysToAppend) {
            if (key != null && gridGainData != null
                    && !gridGainData.isEmpty() && keysToRequest.contains(key) && queryData.containsKey(key)) {
                result.add(queryData.get(key));
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
