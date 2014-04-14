/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class GetMSEdata extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> mseEvent = (Map<String, Object>) tuple.get(0);
        Map<String, Object> location = (Map<String, Object>) mseEvent.get("location");
        String macAddress = location.get("macAddress").toString();

        Map<String, Object> geoCoordinate = (Map<String, Object>) location.get("geoCoordinate");
        Map<String, Object> mapInfo = (Map<String, Object>) location.get("mapInfo");
        String mapHierachy = mapInfo.get("mapHierarchyString").toString();

        Double lattitude = (Double) geoCoordinate.get("lattitude");
        lattitude = (double) Math.round(lattitude * 10000) / 10000;

        Double longitude = (Double) geoCoordinate.get("longitude");
        longitude = (double) Math.round(longitude * 10000) / 10000;
        String locationFormat = lattitude.toString() + "," + longitude.toString();

        Map<String, Object> mseData = new HashMap<>();

        mseData.put("sta_mac_address_latlong", locationFormat);

        String[] zone = mapHierachy.split(">");

        mseData.put("sta_mac_address_campus", zone[0]);
        mseData.put("sta_mac_address_building", zone[1]);
        mseData.put("sta_mac_address_floor", zone[2]);

        collector.emit(new Values(macAddress, mseData));
    }

}
