package net.redborder.storm.state.gridgain;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 03/07/14.
 */
public class GridGainLocationQuery extends GridGainQuery {

    public GridGainLocationQuery(String key) {
        super(key);
    }

    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        if (result == null) {
            Map<String, Object> empty = new HashMap<>();
            collector.emit(new Values(empty));
        } else {
            result.put("dot11_status", "ASSOCIATED");
            collector.emit(new Values(result));
        }
    }
}
