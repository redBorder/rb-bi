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

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> radiusData = (Map<String, Object>) tuple.getValue(0);

        try {
            if (radiusData.containsKey("Called-Station-Id") && radiusData.containsKey("Calling-Station-Id") &&
                    radiusData.containsKey("Framed-IP-Address") && radiusData.containsKey("NAS-Identifier") &&
                    radiusData.containsKey("NAS-IP-Address")) {
                
                String clientMac = radiusData.get("Calling-Station-Id").toString();
                clientMac = clientMac.replace("-", ":");
                
                String calledStation[] = radiusData.get("Called-Station-Id").toString().split(":");
                
                if (calledStation.length != 2) {
                    Logger.getLogger(GetRadiusData.class.getName()).log(Level.WARNING, "Incorrect calledStation format on radius map");
                    return;
                }
                
                String apMac = calledStation[0];
                apMac = apMac.replace("-", ":");
                String ssid = calledStation[1];
                
                String src = radiusData.get("Framed-IP-Address").toString();
                String sensorName = radiusData.get("NAS-Identifier").toString();
                String sensorIP = radiusData.get("NAS-IP-Address").toString();

                Map<String, Object> radiusMap = new HashMap<>();
                Map<String, Object> radiusDruid = new HashMap<>();

                radiusMap.put("ap_mac", apMac);
                radiusMap.put("ssid", ssid);
                
                radiusDruid.putAll(radiusMap);
                radiusDruid.put("client_mac", clientMac);
                radiusDruid.put("src", src);
                radiusDruid.put("sensor_name", sensorName);
                radiusDruid.put("sensor_ip", sensorIP);
                radiusDruid.put("bytes", 0);
                radiusDruid.put("pkts", 0);
                
                collector.emit(new Values(clientMac, radiusMap, radiusDruid));
            }
        } catch (NullPointerException e) {
            Logger.getLogger(GetRadiusData.class.getName()).log(Level.WARNING, "Failed processing a Radius map", e);
        }
    }

}
