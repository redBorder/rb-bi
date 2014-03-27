/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.redborder.storm.trident.state;

import com.redborder.storm.util.KeyUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class twitterUpdater extends BaseStateUpdater<MapState<Map<String, Object>>> {
    
    @Override
    public void updateState(MapState<Map<String, Object>> state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        List<Object> keys = new ArrayList<Object>();
        for (TridentTuple t : tuples) {
            events.add((Map<String, Object>) t.getValueByField("tweetMap"));
            keys.add(t.getValueByField("userTwitterID"));
        }
        state.multiPut(Arrays.asList(keys), events);
    }
    
}
