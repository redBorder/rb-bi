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
    
    boolean debug;
    
    public GetMSEdata(boolean debug){
        this.debug=debug;
    }

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
            location = (Map<String, Object>) mseEventContent.get("location");
            geoCoordinate = (Map<String, Object>) location.get("geoCoordinate");
            mapInfo = (Map<String, Object>) location.get("mapInfo");
            mseData = new HashMap<>();
            mseDataDruid = new HashMap<>();

            macAddress = location.get("macAddress").toString();
            mapHierachy = mapInfo.get("mapHierarchyString").toString();

            lattitude = (Double) geoCoordinate.get("lattitude");
            lattitude = (double) Math.round(lattitude * 100000) / 100000;

            longitude = (Double) geoCoordinate.get("longitude");
            longitude = (double) Math.round(longitude * 100000) / 100000;
            locationFormat = lattitude.toString() + "," + longitude.toString();

            mseData.put("client_lat", lattitude.toString());
            mseData.put("client_long", longitude.toString());
            mseData.put("client_latlong", locationFormat);

            zone = mapHierachy.split(">");

            mseData.put("client_campus", zone[0]);
            mseData.put("client_building", zone[1]);
            mseData.put("client_floor", zone[2]);

            state = location.get("dot11Status").toString();
            mseData.put("dot11_status", "ASSOCIATED");

            if (state.equals("ASSOCIATED")) {
                ArrayList ip = (ArrayList) location.get("ipAddress");
                mseData.put("wireless_id", location.get("ssId").toString());
                mseData.put("wireless_station", location.get("apMacAddress").toString());
                if (ip != null) {
                    mseDataDruid.put("src", ip.get(0).toString());
                }
            }

            mseDataDruid.putAll(mseData);
            mseDataDruid.put("sensor_name", mseEventContent.get("mseUdi"));
            mseDataDruid.put("client_mac", macAddress);
            mseDataDruid.put("bytes", 0);
            mseDataDruid.put("pkts", 0);

            date = new DateTime(mseEventContent.get("timestamp").toString());
            mseDataDruid.put("timestamp", date.withZone(DateTimeZone.UTC).getMillis() / 1000);
            mseDataDruid.put("dot11_status", state);

            collector.emit(new Values(macAddress, mseData, mseDataDruid));
        } catch (NullPointerException e) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed processing a MSE map: \n" + mseEvent.toString(), e);
        }
    }
    
}
