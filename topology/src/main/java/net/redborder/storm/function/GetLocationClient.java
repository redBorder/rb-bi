package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 10/11/14.
 */
public class GetLocationClient extends BaseFunction {

    Map<String, Object> mseEventContent, location;

    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        Map<String, Object> mseEvent = (Map<String, Object>) tuple.get(0);
        mseEventContent = (Map<String, Object>) mseEvent.get("StreamingNotification");
        location = (Map<String, Object>) mseEventContent.get("location");
        String macAddress = (String) location.get("macAddress");

        Map<String, Object> map = new HashMap<>();
        map.put("client_mac", macAddress);
        tridentCollector.emit(new Values(map));
    }
}
