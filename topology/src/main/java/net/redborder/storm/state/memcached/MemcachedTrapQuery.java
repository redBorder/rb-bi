package net.redborder.storm.state.memcached;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 07/10/14.
 */
public class MemcachedTrapQuery extends MemcachedQuery{

    public MemcachedTrapQuery(String key, String generalKey) {
        super(key, generalKey);
    }


    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        if (result == null) {
            Map<String, Object> empty = new HashMap<>();
            empty.put("client_rssi", "unknown");
            empty.put("client_rssi_num", 0);
            empty.put("client_snr", "unknown");
            empty.put("client_snr_num", 0);
            collector.emit(new Values(empty));
        } else {
            collector.emit(new Values(result));
        }
    }
}
