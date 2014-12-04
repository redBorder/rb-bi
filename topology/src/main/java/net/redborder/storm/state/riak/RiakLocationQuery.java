package net.redborder.storm.state.riak;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
        Map<String, Object> nmspEvent = (Map<String, Object>) tuple;
        String client_mac = (String) nmspEvent.get("client_mac");
        List<String> apMacs = (List<String>) nmspEvent.get("apmac");
        List<Integer> clientRssis = (List<Integer>) nmspEvent.get("rssi");
        Integer rssi = Collections.min(clientRssis);
        String apMac = apMacs.get(clientRssis.indexOf(rssi));

        Map<String, Object> map = new HashMap<>();

        if (result == null) {
            map.put("client_mac", client_mac);
            map.put("client_rssi", rssi);
            map.put("wireless_station", apMac);
            map.put("dot11status", "PROBING");
        } else {
            if (apMacs.contains(result.get("wireless_station"))) {
                map.put("client_rssi", rssi);
                map.putAll(result);
            } else {
                map.put("client_mac", client_mac);
                map.put("client_rssi", rssi);
                map.put("wireless_station", apMac);
                map.put("dot11status", "PROBING");
            }
        }
        collector.emit(new Values(map));
    }
}
