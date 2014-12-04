package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 4/12/14.
 */
public class GetNMSPInfoData extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> map = (Map<String, Object>) tuple.get(0);
        if (map != null) {
            Map<String, Object> data = new HashMap<>();
            data.putAll(map);
            data.remove("client_mac");

            Map<String, Object> druid = new HashMap<>();
            druid.put("bytes", 0);
            druid.put("pkts", 0);
            druid.put("timestamp", System.currentTimeMillis() / 1000);
            druid.putAll(map);

            collector.emit(new Values(map.get("client_mac"), data, druid));
        }
    }
}
