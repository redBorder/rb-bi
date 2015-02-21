package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 16/2/15.
 */
public class ProcessMse10Association extends BaseFunction {


    Map<Integer, String> cache;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        cache = new HashMap<>();
        cache.put(0, "IDLE");
        cache.put(1, "AAA_PENDING");
        cache.put(2, "AUTHENTICATED");
        cache.put(3, "ASSOCIATED");
        cache.put(4, "POWERSAVE");
        cache.put(5, "DISASSOCIATED");
        cache.put(6, "TO_BE_DELETED");
        cache.put(7, "PROBING");
        cache.put(8, "BLACK_LISTED");
        cache.put(256, "WAIT_AUTHENTICATED");
        cache.put(257, "WAIT_ASSOCIATED");
    }


    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> association = (Map<String, Object>) tuple.get(0);
        try {
            Map<String, Object> dataToSave = new HashMap<>();
            Map<String, Object> dataToDruid = new HashMap<>();
            String client_mac = (String) association.get("deviceId");

            if (association.get("ssid") != null)
                dataToSave.put("wireless_id", association.get("ssid"));

            if (association.get("band") != null)
                dataToSave.put("dot11_protocol", association.get("band"));

            if (association.get("status") != null)
                dataToSave.put("dot11_status", cache.get(association.get("status")));

            if (association.get("apMacAddress") != null)
                dataToSave.put("wireless_station", association.get("apMacAddress"));

            if (!association.get("username").equals(""))
                dataToSave.put("client_id", association.get("username"));


            dataToDruid.putAll(dataToSave);
            dataToDruid.put("sensor_name", association.get("subscriptionName"));
            dataToDruid.put("client_mac", client_mac);
            dataToDruid.put("timestamp", ((Long) association.get("timestamp")) / 1000L);
            dataToDruid.put("bytes", 0);
            dataToDruid.put("pkts", 0);
            dataToDruid.put("type", "mse10");

            collector.emit(new Values(client_mac, dataToSave, dataToDruid));
        } catch (Exception ex) {
            if (association != null)
                System.out.println("MSE10 association event dropped: " + association.toString());
            ex.printStackTrace();
        }
    }
}
