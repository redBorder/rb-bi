package net.redborder.storm.state.memcached;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 03/09/14.
 */
public class MemcachedLocationV10Query extends MemcachedQuery {


    public MemcachedLocationV10Query(String key, String generalKey) {
        super(key, generalKey);
    }

    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        if (result == null) {
            Map<String, Object> empty = new HashMap<>();
            empty.put("dot11_status", "PROBING");
            collector.emit(new Values(empty));
        } else {
            collector.emit(new Values(result));
        }
    }
}
