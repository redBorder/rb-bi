package net.redborder.storm.function;


import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by andresgomez on 16/2/15.
 */
public class GetLocationUpdateClientMac extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> mseUpdate = (Map<String, Object>) tuple.get(0);
        collector.emit(new Values(mseUpdate.get("deviceId")));
    }
}
