package net.redborder.storm.state.gridgain;

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
public class GridGainNmspMeasureQuery extends GridGainQuery {

    public GridGainNmspMeasureQuery(String key) {
        super(key);
    }

    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        Map<String, Object> nmspEvent = (Map<String, Object>) tuple.get(0);

        String client_mac = (String) nmspEvent.get("client_mac");
        List<String> apMacs = (List<String>) nmspEvent.get("ap_mac");
        List<Integer> clientRssis = (List<Integer>) nmspEvent.get("rssi");
        Integer rssi = Collections.min(clientRssis);
        String apMac = apMacs.get(clientRssis.indexOf(rssi));

        Map<String, Object> map = new HashMap<>();

        if (rssi == 0)
            map.put("client_rssi", "unknown");
        else if (rssi <= -85)
            map.put("client_rssi", "bad");
        else if (rssi <= -80)
            map.put("client_rssi", "low");
        else if (rssi <= -70)
            map.put("client_rssi", "medium");
        else if (rssi <= -60)
            map.put("client_rssi", "good");
        else
            map.put("client_rssi", "excelent");

        if (result == null) {
            map.put("client_rssi_num", rssi);
            map.put("wireless_station", apMac);
            map.put("dot11status", "PROBING");
        } else {
            if (apMacs.contains(result.get("wireless_station"))) {
                map.put("client_rssi_num", rssi);
                map.putAll(result);
            } else {
                map.put("client_rssi_num", rssi);
                map.put("wireless_station", apMac);
                map.put("dot11status", "PROBING");
            }
        }
        Map<String, Object> druid = new HashMap<>();
        druid.put("bytes", 0);
        druid.put("pkts", 0);
        druid.put("timestamp", System.currentTimeMillis()/1000);
        druid.putAll(map);
        collector.emit(new Values(client_mac, map, druid));
    }
}
