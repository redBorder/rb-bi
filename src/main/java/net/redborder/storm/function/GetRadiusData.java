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
 * @author andresgomez
 */
public class GetRadiusData extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> radiusData = (Map<String, Object>) tuple.getValue(0);
        Map<String, Object> radiusDruid = new HashMap<>();
        Map<String, Object> radiusCached;
        String timestamp = null;

        if (tuple.size() > 1) {
            radiusCached = (Map<String, Object>) tuple.getValue(1);
        } else {
            radiusCached = new HashMap<>();
        }

        Map<String, Object> radiusMap = radiusCached;

        if (radiusData.containsKey("timestamp")) {
            timestamp = radiusData.get("timestamp").toString();
        } else {
            Logger.getLogger(GetRadiusData.class.getName()).log(Level.WARNING, "Radius event hasn't timestamp!");
            return;
        }

        try {
            Object clientMacObject = radiusData.get("Calling-Station-Id");

            if (clientMacObject != null) {
                String apMac = null;
                String ssid = null;
                String clientId = null;
                String clientMac = clientMacObject.toString();

                clientMac = clientMac.replace("-", ":");

                radiusDruid.put("client_mac", clientMac);

                Object calledStationObject = radiusData.get("Called-Station-Id");
                if (calledStationObject != null) {
                    String calledStation[] = calledStationObject.toString().split(":");

                    if (calledStation.length != 2) {
                        Logger.getLogger(GetRadiusData.class.getName()).log(Level.WARNING, "Incorrect calledStation format on radius map");
                    } else {
                        apMac = calledStation[0];
                        apMac = apMac.replace("-", ":");
                        ssid = calledStation[1];

                        radiusMap.put("ap_mac", apMac);
                        radiusMap.put("wlan_ssid", ssid);
                    }
                }

                Object srcObject = radiusData.get("Framed-IP-Address");
                if (srcObject != null) {
                    String src = srcObject.toString();
                    radiusDruid.put("src", src);
                }

                Object sensorNameObject = radiusData.get("NAS-Identifier");
                if (sensorNameObject != null) {
                    String sensorName = sensorNameObject.toString();
                    radiusDruid.put("sensor_name", sensorName);
                }

                Object sensorIpObject = radiusData.get("NAS-IP-Address");
                if (sensorIpObject != null) {
                    String sensorIP = sensorIpObject.toString();
                    radiusDruid.put("sensor_ip", sensorIP);
                }

                Object clientIdObject = radiusData.get("User-Name");
                if (clientIdObject != null) {
                    clientId = clientIdObject.toString();
                    radiusMap.put("client_id", clientId);
                }

                radiusDruid.putAll(radiusMap);
                radiusDruid.put("timestamp", timestamp);
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
