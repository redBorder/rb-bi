/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        Map<String, Object> mseEventContent, location, geoCoordinate, mapInfo, mseData, mseDataDruid;
        String macAddress, mapHierachy, locationFormat, state;
        Double lattitude, longitude;
        DateTime date;
        String[] zone;
        
        try {
            mseEventContent = (Map<String, Object>) mseEvent.get("StreamingNotification");
            location        = (Map<String, Object>) mseEventContent.get("location");
            geoCoordinate   = (Map<String, Object>) location.get("geoCoordinate");
            mapInfo         = (Map<String, Object>) location.get("mapInfo");
            mseData         = new HashMap<>();
            mseDataDruid    = new HashMap<>();

            macAddress = location.get("macAddress").toString();
            mapHierachy = mapInfo.get("mapHierarchyString").toString();

            lattitude = (Double) geoCoordinate.get("lattitude");
            lattitude = (double) Math.round(lattitude * 10000) / 10000;

            longitude = (Double) geoCoordinate.get("longitude");
            longitude = (double) Math.round(longitude * 10000) / 10000;
            locationFormat = lattitude.toString() + "," + longitude.toString();        

            mseData.put("sta_mac_address_lat", lattitude.toString());
            mseData.put("sta_mac_address_long", longitude.toString());
            mseData.put("sta_mac_address_latlong", locationFormat);

            zone = mapHierachy.split(">");

            mseData.put("sta_mac_address_campus", zone[0]);
            mseData.put("sta_mac_address_building", zone[1]);
            mseData.put("sta_mac_address_floor", zone[2]);

            mseDataDruid.putAll(mseData);
            mseDataDruid.put("sensor_name", mseEventContent.get("mseUdi"));
            mseDataDruid.put("client_mac", macAddress);
            mseDataDruid.put("bytes", 0);
            mseDataDruid.put("pkts", 0);

            date = new DateTime(mseEventContent.get("timestamp").toString());
            mseDataDruid.put("timestamp", date.withZone(DateTimeZone.UTC).getMillis()/1000);

            state = location.get("dot11Status").toString();
            mseDataDruid.put("dot11_status", state);

            if(state.equals("ASSOCIATED")){
                ArrayList ip = (ArrayList) location.get("ipAddress");
                mseDataDruid.put("wlan_ssid", location.get("ssId").toString());
                mseDataDruid.put("ap_mac", location.get("apMacAddress").toString());
                mseDataDruid.put("band", location.get("band").toString());
                mseDataDruid.put("src", ip.get(0).toString());
            }
            collector.emit(new Values(macAddress, mseData, mseDataDruid));
        } catch (NullPointerException e) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed reading a MSE JSON tuple", e);
        }
    }

}
