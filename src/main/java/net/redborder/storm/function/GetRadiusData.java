/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class GetRadiusData extends BaseFunction {
    
    boolean debug;
    
    public GetRadiusData(boolean debug){
        this.debug=debug;
    }        

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> radiusData = (Map<String, Object>) tuple.getValue(0);
        //Map<String, Object> radiusCached = (Map<String, Object>) tuple.getValue(1);

        try {
            if (radiusData.containsKey("Calling-Station-Id")) {

                String apMac = null;
                String ssid = null;
                String clientMac = null;
                String clientId = null;

                clientMac = radiusData.get("Calling-Station-Id").toString();
                clientMac = clientMac.replace("-", ":");

                if (radiusData.containsKey("Called-Station-Id")) {
                    String calledStation[] = radiusData.get("Called-Station-Id").toString().split(":");

                    if (calledStation.length != 2) {
                        Logger.getLogger(GetRadiusData.class.getName()).log(Level.WARNING, "Incorrect calledStation format on radius map");
                        return;
                    }

                    apMac = calledStation[0];
                    apMac = apMac.replace("-", ":");
                    ssid = calledStation[1];

                }

                Map<String, Object> radiusDruid = new HashMap<>();

                if (radiusData.containsKey("Framed-IP-Address")) {
                    String src = radiusData.get("Framed-IP-Address").toString();
                    radiusDruid.put("src", src);
                }

                if (radiusData.containsKey("NAS-Identifier")) {
                    String sensorName = radiusData.get("NAS-Identifier").toString();
                    radiusDruid.put("sensor_name", sensorName);
                }

                if (radiusData.containsKey("NAS-IP-Address")) {
                    String sensorIP = radiusData.get("NAS-IP-Address").toString();
                    radiusDruid.put("sensor_ip", sensorIP);
                }

                String timestamp = radiusData.get("timestamp").toString();
                if (radiusData.containsKey("User-Name")) {
                    clientId = radiusData.get("User-Name").toString();
                }

                Map<String, Object> radiusMap = new HashMap<>();;

                if (apMac != null) {
                    radiusMap.put("ap_mac", apMac);
                }

                if (ssid != null) {
                    radiusMap.put("wlan_ssid", ssid);
                }

                if (clientId != null) {
                    radiusMap.put("client_id", clientId);
                }

                radiusDruid.putAll(radiusMap);
                radiusDruid.put("timestamp", timestamp);
                radiusDruid.put("client_mac", clientMac);
                radiusDruid.put("bytes", 0);
                radiusDruid.put("pkts", 0);

                collector.emit(new Values(clientMac, radiusMap, radiusDruid));
            } else {
                System.out.println("Event drop: Not found client mac on radius event!");
            }
        } catch (NullPointerException e) {
            Logger.getLogger(GetRadiusData.class.getName()).log(Level.WARNING, "Failed processing a Radius map", e);
        }
    }

}
