/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class ThroughputLoggingFilter extends BaseFilter {

    Map<Long, Integer> _count;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _count = new HashMap<>();
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Long timestamp = System.currentTimeMillis() / 1000;
        
        if(_count.containsKey(timestamp)) {
            Integer count = _count.get(timestamp);
            _count.put(timestamp, count + 1);
        } else {
            if (_count.containsKey(timestamp - 1))
                System.out.println(_count.get(timestamp - 1));
            
            _count.put(timestamp, 1);
        }
        
        return true;
    }
}
