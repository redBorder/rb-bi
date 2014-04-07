/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.state.updater;

import com.google.common.collect.Lists;
import net.redborder.storm.util.state.KeyUtils;
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
        List<Map<String, Object>> events = Lists.newArrayList();
        List<List<Object>> keys = Lists.newArrayList();
        for (TridentTuple t : tuples) {
            List<Object> l = Lists.newArrayList();
            l.add(t.getValueByField("userTwitterID"));
            keys.add(l);
            events.add((Map<String, Object>) t.getValueByField("tweetMap"));
            System.out.println("Twiiter: " + t.getValueByField("userTwitterID"));
        }
        state.multiPut(keys, events);
    }
    
}
