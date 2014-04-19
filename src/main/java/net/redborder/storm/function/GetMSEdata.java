/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
        Map<String, Object> mseEventContent = (Map<String, Object>) mseEvent.get("StreamingNotification");
        Map<String, Object> location = (Map<String, Object>) mseEventContent.get("location");
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

        mseData.put("sta_mac_address_lat",lattitude.toString());
        mseData.put("sta_mac_address_long", longitude.toString());
        mseData.put("sta_mac_address_latlong", locationFormat);

        String[] zone = mapHierachy.split(">");

        mseData.put("sta_mac_address_campus", zone[0]);
        mseData.put("sta_mac_address_building", zone[1]);
        mseData.put("sta_mac_address_floor", zone[2]);
        
        Map<String,Object> mseDataDruid = new HashMap<>();
        
        mseDataDruid.putAll(mseData);
        mseDataDruid.put("client-mac", macAddress);
        mseDataDruid.put("bytes", 0);
        mseDataDruid.put("pkts", 0);
        
        DateTime date = new DateTime(mseEventContent.get("timestamp").toString());
        System.out.println(date.withZone(DateTimeZone.UTC).getMillis()/1000);
        
        mseDataDruid.put("timestamp", zone);
        
        String state = location.get("dot11Status").toString();
        
        if(state.equals("ASSOCIATED")){
            mseDataDruid.put("wlan_ssid", location.get("ssId").toString());
            mseDataDruid.put("ap_mac", location.get("apMacAddress").toString());
            mseDataDruid.put("band", location.get("band").toString());
            
            String [] ip = (String[]) location.get("ipAddress");
            
            mseDataDruid.put("src", ip[0]);
            mseDataDruid.put("dot11_status", location.get("dot11Status"));
        }else{
            mseDataDruid.put("dot11_status", location.get("dot11Status"));
        }

        collector.emit(new Values(macAddress, mseData, mseDataDruid));
    }

}
