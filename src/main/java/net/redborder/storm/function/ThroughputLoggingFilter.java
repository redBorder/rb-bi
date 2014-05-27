/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import java.util.HashMap;
import java.util.Map;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class ThroughputLoggingFilter extends BaseFilter {

    Map<Long, Integer> _count;
    double _mean;
    int _sec_count;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _count = new HashMap<>();
        _sec_count = 0;
        _mean = 0;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Long timestamp = System.currentTimeMillis() / 1000;
        
        if(_count.containsKey(timestamp)) {
            Integer count = _count.get(timestamp);
            _count.put(timestamp, count + 1);
        } else {
            if (_count.containsKey(timestamp - 1)) {
                int msgs = _count.get(timestamp - 1);
                System.out.println(msgs);
                
                if (_sec_count <= 60) {
                    _mean += msgs;
                } else {
                    _mean = _mean / 60;
                    System.out.println("STATS THROUGHPUT - MEAN " + _mean);
                    _sec_count = 0;
                    _mean = 0;
                }
            }
            
            _count.put(timestamp, 1);
            _sec_count += 1;
        }
        
        return true;
    }
}
