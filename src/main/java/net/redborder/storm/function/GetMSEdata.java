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
        Map<String, Object> mseEventContent, location, geoCoordinate = null, mapInfo, mseData, mseDataDruid;
        String mapHierachy, locationFormat, state;
        String macAddress = null;
        String dateString = null;
        Double lattitude, longitude;
        String[] zone;

        try {
            mseEventContent = (Map<String, Object>) mseEvent.get("StreamingNotification");
            location = (Map<String, Object>) mseEventContent.get("location");
            mseData = new HashMap<>();
            mseDataDruid = new HashMap<>();

            if (location != null) {
                geoCoordinate = (Map<String, Object>) location.get("geoCoordinate");
                mapInfo = (Map<String, Object>) location.get("mapInfo");
                macAddress = (String) location.get("macAddress");
                mseDataDruid.put("client_mac", macAddress);

                mapHierachy = (String) mapInfo.get("mapHierarchyString");

                if(mapHierachy!=null) {
                    zone = mapHierachy.split(">");

                    mseData.put("client_campus", zone[0]);
                    mseData.put("client_building", zone[1]);
                    mseData.put("client_floor", zone[2]);
                }

                state = (String) location.get("dot11Status");
                mseDataDruid.put("dot11_status", state);
                mseData.put("dot11_status", state);

                if (state != null && state.equals("ASSOCIATED")) {
                    ArrayList ip = (ArrayList) location.get("ipAddress");
                    mseData.put("wireless_id", location.get("ssId"));
                    mseData.put("wireless_station", location.get("apMacAddress"));
                    if (ip != null && ip.get(0) != null) {
                        mseDataDruid.put("src", ip.get(0));
                    }
                }
            }

            if (geoCoordinate != null) {
                lattitude = (Double) geoCoordinate.get("lattitude");
                lattitude = (double) Math.round(lattitude * 100000) / 100000;

                longitude = (Double) geoCoordinate.get("longitude");
                longitude = (double) Math.round(longitude * 100000) / 100000;

                locationFormat = lattitude.toString() + "," + longitude.toString();

                mseData.put("client_lat", lattitude.toString());
                mseData.put("client_long", longitude.toString());
                mseData.put("client_latlong", locationFormat);
            }

            if (mseEventContent != null) {
                dateString = (String) mseEventContent.get("timestamp");
                String sensorName = (String) mseEventContent.get("mseUdi");

                if (sensorName != null) {
                    mseDataDruid.put("sensor_name", sensorName);
                }
            }

            mseDataDruid.putAll(mseData);
            mseDataDruid.put("bytes", 0);
            mseDataDruid.put("pkts", 0);

            if (dateString != null && macAddress != null) {
                mseDataDruid.put("timestamp", new DateTime(dateString).withZone(DateTimeZone.UTC).getMillis() / 1000);
                collector.emit(new Values(macAddress, mseData, mseDataDruid));
            }

        } catch (Exception  e) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed processing a MSE map: \n" + mseEvent.toString(), e);
        }
    }
    
}
