/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.state.memcached;

import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
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
public class MemcachedQuery extends BaseQueryFunction<MapState<Map<String, Object>>, Map<String, Object>> {

    String _key;
    String _generalkey;

    public MemcachedQuery(String key) {
        _key = key;
        _generalkey = "rbbi:none:";
    }
    
    public MemcachedQuery(String key, String generalKey) {
        _key = key;
        _generalkey="rbbi:"+generalKey+":";
    }

    @Override
    public List<Map<String, Object>> batchRetrieve(MapState<Map<String, Object>> state, List<TridentTuple> tuples) {
        List<Map<String, Object>> memcachedData = null;
        List<Map<String, Object>> result = Lists.newArrayList();
        List<Object> keysToRequest = Lists.newArrayList();
        List<String> keysToAppend = Lists.newArrayList();
        
        for (TridentTuple t : tuples) {
            Map<String, Object> flow = (Map<String, Object>) t.getValue(0);
            if(flow!=null) {
                String key = (String) flow.get(_key);
                keysToAppend.add(_generalkey + key);

                if (!key.equals("null") && !keysToRequest.contains(_generalkey + key)) {
                    keysToRequest.add(_generalkey + key);
                }
            }else {
                keysToAppend.add(_generalkey + null);
            }
        }
        
        // System.out.println("BatchSize " + tuples.size() +
         //           " RequestedToMemcached: " + keysToRequest.size());
        
        if (!keysToRequest.isEmpty()) {
            List<List<Object>> keysToMemcached = Lists.newArrayList();
        
            for (Object key : keysToRequest) {
                List<Object> l = Lists.newArrayList();
                l.add(key);
                keysToMemcached.add(l);
            }
            
            try {
                memcachedData = state.multiGet(keysToMemcached);
               //  System.out.println("MemcachedResponse: " + memcachedData.toString());
            } catch (ReportedFailedException e) {
                Logger.getLogger(MemcachedQuery.class.getName()).log(Level.WARNING, null, e);
            }
        }
        
        for (String key : keysToAppend) {
            if (memcachedData != null && !memcachedData.isEmpty() &&keysToRequest.contains(key)) {
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
            Map<String,Object> empty = new HashMap<>();
            collector.emit(new Values(empty));
        } else {
            collector.emit(new Values(result));
        }
    }

}