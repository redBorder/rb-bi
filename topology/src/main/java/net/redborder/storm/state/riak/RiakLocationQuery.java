package net.redborder.storm.state.riak;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 03/07/14.
 */
public class RiakLocationQuery extends RiakQuery {

    public RiakLocationQuery(String key) {
        super(key);
    }

    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        if (result == null) {
            Map<String, Object> empty = new HashMap<>();
            //empty.put("quality", 100);
            collector.emit(new Values(empty));
        } else {
            result.put("dot11_status", "ASSOCIATED");
            collector.emit(new Values(result));
        }
    }
}
