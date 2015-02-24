/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import net.redborder.storm.util.logger.RbLogger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>This function analyzes the MSE events and get interest fields.</p>
 *
 * @author Andres Gomez
 */
public class GetMSEdata extends BaseFunction {

    Logger logger;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        logger = RbLogger.getLogger(GetMSEdata.class.getName());
    }

    /**
     * <p>This function analyzes the events of MSE and get interest fields.</p>
     */
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
            if(mseEvent.containsKey("StreamingNotification")) {
                mseEventContent = (Map<String, Object>) mseEvent.get("StreamingNotification");
                location = (Map<String, Object>) mseEventContent.get("location");
                mseData = new HashMap<>();
                mseDataDruid = new HashMap<>();

                if (location != null) {
                    geoCoordinate = (Map<String, Object>) location.get("GeoCoordinate");
                    if (geoCoordinate == null) {
                        geoCoordinate = (Map<String, Object>) location.get("geoCoordinate");
                    }
                    mapInfo = (Map<String, Object>) location.get("MapInfo");
                    if (mapInfo == null) {
                        mapInfo = (Map<String, Object>) location.get("mapInfo");
                    }
                    macAddress = (String) location.get("macAddress");
                    mseDataDruid.put("client_mac", macAddress);

                    mapHierachy = (String) mapInfo.get("mapHierarchyString");

                    if (mapHierachy != null) {
                        zone = mapHierachy.split(">");

                        if (zone.length >= 1)
                            mseData.put("client_campus", zone[0]);
                        if (zone.length >= 2)
                            mseData.put("client_building", zone[1]);
                        if (zone.length >= 3)
                            mseData.put("client_floor", zone[2]);
                    }

                    state = (String) location.get("dot11Status");
                    mseDataDruid.put("dot11_status", state);
                    mseData.put("dot11_status", state);

                    if (state != null && state.equals("ASSOCIATED")) {
                        ArrayList ip = (ArrayList) location.get("ipAddress");
                        if (location.get("ssId") != null)
                            mseData.put("wireless_id", location.get("ssId"));
                        if (location.get("apMacAddress") != null)
                            mseData.put("wireless_station", location.get("apMacAddress"));
                        if (ip != null && ip.get(0) != null) {
                            mseDataDruid.put("src", ip.get(0));
                        }
                    }
                }

                if (geoCoordinate != null) {
                    lattitude = (Double) geoCoordinate.get("latitude");
                    if (lattitude == null) {
                        lattitude = (Double) geoCoordinate.get("lattitude");
                    }
                    lattitude = (double) Math.round(lattitude * 100000) / 100000;

                    longitude = (Double) geoCoordinate.get("longitude");
                    longitude = (double) Math.round(longitude * 100000) / 100000;

                    locationFormat = lattitude.toString() + "," + longitude.toString();

                    mseData.put("client_latlong", locationFormat);
                }

                if (mseEventContent != null) {
                    dateString = (String) mseEventContent.get("timestamp");
                    String sensorName = (String) mseEventContent.get("subscriptionName");

                    if (sensorName != null) {
                        mseDataDruid.put("sensor_name", sensorName);
                    }
                }

                mseDataDruid.putAll(mseData);
                mseDataDruid.put("client_rssi", "unknown");
                mseDataDruid.put("client_rssi_num", 0);
                mseDataDruid.put("client_snr", "unknown");
                mseDataDruid.put("client_snr_num", 0);
                mseDataDruid.put("bytes", 0);
                mseDataDruid.put("pkts", 0);
                mseDataDruid.put("type", "mse");

                if (dateString != null && macAddress != null) {
                    mseDataDruid.put("timestamp", new DateTime(dateString).withZone(DateTimeZone.UTC).getMillis() / 1000);
                    collector.emit(new Values(macAddress, mseData, mseDataDruid, null));
                } else {
                    mseDataDruid.put("timestamp", System.currentTimeMillis() / 1000);
                    collector.emit(new Values(macAddress, mseData, mseDataDruid, null));
                }
            }else{
                logger.severe("MSE event is a 10 version, emitting: [null, null, null, " + mseEvent.size() + "]");
                collector.emit(new Values(null, null, null, mseEvent));
            }
        } catch (NullPointerException e) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed processing a MSE map: \n" + mseEvent.toString());
            e.printStackTrace();
        }
    }

}
