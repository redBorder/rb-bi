package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 16/2/15.
 */
public class ProcessMse10LocationUpdate extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> locationUpdate = (Map<String, Object>) tuple.get(0);
        Map<String, Object> dataToSave = new HashMap<>();
        Map<String, Object> dataToDruid = new HashMap<>();

        String client_mac = (String) locationUpdate.get("deviceId");

        String locationMapHierarchy = (String) locationUpdate.get("locationMapHierarchy");

        if (locationMapHierarchy != null) {
            String[] locations = locationMapHierarchy.split(">");

            if (locations.length >= 1)
                dataToSave.put("client_campus", locations[0]);
            if (locations.length >= 2)
                dataToSave.put("client_building", locations[1]);
            if (locations.length >= 3)
                dataToSave.put("client_floor", locations[2]);
            if (locations.length >= 4)
                dataToSave.put("client_zone", locations[3]);
        }

        dataToDruid.putAll(dataToSave);
        dataToDruid.put("sensor_name", locationUpdate.get("subscriptionName"));

        if (locationUpdate.get("timestamp") instanceof Integer)
            dataToDruid.put("timestamp", ((Integer) locationUpdate.get("timestamp")) / 1000L);
        else if (locationUpdate.get("timestamp") instanceof Long)
            dataToDruid.put("timestamp", ((Long) locationUpdate.get("timestamp")) / 1000L);
        else
            dataToDruid.put("timestamp", System.currentTimeMillis() / 1000L);


        dataToDruid.put("bytes", 0);
        dataToDruid.put("pkts", 0);
        dataToDruid.put("type", "mse10");

        collector.emit(new Values(client_mac, dataToSave, dataToDruid));
    }
}
