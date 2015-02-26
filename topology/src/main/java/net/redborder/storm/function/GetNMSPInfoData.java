package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import net.redborder.storm.util.logger.RbLogger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 4/12/14.
 */
public class GetNMSPInfoData extends BaseFunction {
    Logger logger;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        logger = RbLogger.getLogger(GetNMSPInfoData.class.getName());
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> map = (Map<String, Object>) tuple.get(0);

        if (map != null) {
            Map<String, Object> enrichment = (Map<String, Object>) map.remove("enrichment");

            Map<String, Object> data = new HashMap<>();
            data.putAll(map);
            data.remove("client_mac");

            Map<String, Object> druid = new HashMap<>();
            druid.put("bytes", 0);
            druid.put("pkts", 0);
            druid.put("type", "nmsp");
            druid.put("timestamp", System.currentTimeMillis() / 1000);
            druid.putAll(map);

            if (enrichment != null)
                druid.putAll(enrichment);

            Object vlan = map.get("vlan_id");

            if (vlan != null) {
                druid.put("src_vlan", vlan);
                data.put("src_vlan", vlan);
                data.remove("vlan_id");
                druid.remove("vlan_id");
            }

            // logger.severe("Processed NMSP data info, emitting  [" + map.get("client_mac") + ", " + data.size() + ", " + druid.size() + "]");

            collector.emit(new Values(map.get("client_mac"), data, druid));
        }
    }
}
