/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redborder.storm.trident.updater;

import com.google.common.collect.Lists;
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
public class mseUpdater extends BaseStateUpdater<MapState<Map<String, Object>>> {

    @Override
    public void updateState(MapState<Map<String, Object>> state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Map<String, Object>> events = Lists.newArrayList();
        List<List<Object>> keys = Lists.newArrayList();
        for (TridentTuple t : tuples) {
            List<Object> l = Lists.newArrayList();
            l.add(t.getValueByField("mac_src_mse"));
            System.out.println("MAC-LOC: " + t.getValueByField("mac_src_mse"));
            keys.add(l);
            events.add((Map<String, Object>) t.getValueByField("geoLocationMSE"));
            System.out.println(t.getValueByField("geoLocationMSE"));
        }
        state.multiPut(keys, events);
    }

}
