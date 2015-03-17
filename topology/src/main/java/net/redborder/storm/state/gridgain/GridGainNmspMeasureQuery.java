package net.redborder.storm.state.gridgain;

import backtype.storm.tuple.Values;
import net.redborder.storm.util.logger.RbLogger;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 03/07/14.
 */
public class GridGainNmspMeasureQuery extends GridGainQuery {

    public GridGainNmspMeasureQuery(String key) {
        super(key);
    }

    Logger logger;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        logger = RbLogger.getLogger(GridGainNmspMeasureQuery.class.getName());
    }

    @Override
    public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
        Map<String, Object> nmspEvent = (Map<String, Object>) tuple.get(0);

        if (nmspEvent != null) {
            // if (result != null)
            //     logger.severe("Resulting NmspGridGainQuery " + result.size());
            // else
            //     logger.severe("Resulting NmspGridGainQuery " + null);

            List<String> apMacs = (List<String>) nmspEvent.get("ap_mac");
            List<Integer> clientRssis = (List<Integer>) nmspEvent.get("rssi");

            if (clientRssis != null && apMacs != null && !apMacs.isEmpty() && !clientRssis.isEmpty()) {
                String client_mac = (String) nmspEvent.get("client_mac");
                String sensor_name = (String) nmspEvent.get("sensor_name");
                Integer rssi = Collections.max(clientRssis);
                // logger.severe("Max RSSI is: " + rssi);
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
                    map.put("dot11_status", "PROBING");
                } else {
                    String apAssociated = (String) result.get("wireless_station");

                    if (apMacs.contains(apAssociated)) {
                        Integer rssiAssociated = clientRssis.get(apMacs.indexOf(apAssociated));
                        map.put("client_rssi_num", rssiAssociated);
                        map.putAll(result);
                    } else {
                        map.put("client_rssi_num", rssi);
                        map.put("wireless_station", apMac);
                        map.put("dot11_status", "PROBING");
                    }
                }

                Map<String, Object> druid = new HashMap<>();

                druid.put("bytes", 0);
                druid.put("pkts", 0);
                druid.put("sensor_name", sensor_name);
                druid.put("type", "nmsp");
                druid.put("client_mac", client_mac);
                druid.put("timestamp", System.currentTimeMillis() / 1000);
                druid.putAll(map);

                // logger.severe("Finished nmspGridGainQuery, emmiting [" + client_mac + ", " + map.size() + ", " + druid.size() + "]");
                collector.emit(new Values(client_mac, map, druid));
            }
        }
    }
}