/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.state.memcached;

import backtype.storm.topology.ReportedFailedException;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class MemcachedUpdater extends BaseStateUpdater<MapState<Map<String, Object>>> {
    String _key;
    String _value;
    String _generalKey;
    
    public MemcachedUpdater(String key, String value){
        _key=key;
        _value=value;
        _generalKey="rbbi:none:";
    }
    
    public MemcachedUpdater(String key, String value, String generalKey){
        this(key, value);
        _generalKey="rbbi:" + generalKey + ":";
    }

    @Override
    public void updateState(MapState<Map<String, Object>> state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Map<String, Object>> events = Lists.newArrayList();
        List<List<Object>> keys = Lists.newArrayList();
        for (TridentTuple t : tuples) {
            List<Object> l = Lists.newArrayList();
            l.add(_generalKey+t.getValueByField(_key));
            keys.add(l);
            events.add((Map<String, Object>) t.getValueByField(_value));
            
            //System.out.println("SAVED TO MEMCACHED KEY: " + _generalKey +t.getValueByField(_key) +
              //      " VALUE: " + t.getValueByField(_value));
        }
        
        try {
            state.multiPut(keys, events);
        } catch (ReportedFailedException e) {
            Logger.getLogger(MemcachedUpdater.class.getName()).log(Level.WARNING, null, e);
        }
    }

}
