/*
 *  * To change this license header, choose License Headers in Project Properties.
 *   * To change this template file, choose Tools | Templates
 *    * and open the template in the editor.
 *     */
package net.redborder.storm.function;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

public class ThroughputLoggingFilter extends BaseFilter {

    Map<Long, Integer> _count;
    double _mean;
    double _totalMean;
    int _sec_count;
    String _position;

    public ThroughputLoggingFilter(String position){
        this._position=position;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _count = new HashMap<>();
        _sec_count = 0;
        _mean = 0;
        _totalMean = 0;
        System.out.println("Starting throughput metrics [ " + _position + " ]");
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
                //System.out.println(msgs);

                if (_sec_count <= 60) {
                    _mean += msgs;
                } else {
                    _mean = _mean / 60;

                    if (_totalMean == 0) {
                        _totalMean = _mean;
                    } else {
                        _totalMean = (_totalMean + _mean) / 2;
                    }

                    System.out.println("STATS THROUGHPUT [ "+ _position +" ] - MINUTE MEAN " + _mean);
                    System.out.println("STATS THROUGHPUT [ "+ _position +" ] - TOTAL MEAN " + _totalMean);

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