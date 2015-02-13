package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 10/11/14.
 */
public class LocationLogicMse extends BaseFunction {

    Map<String, Object> mseEventContent, location, mapInfo;

    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        Map<String, Object> mseEvent = (Map<String, Object>) tuple.getValueByField("mse_map");
        if (mseEvent != null) {
            Map<String, Object> cacheLocation = (Map<String, Object>) tuple.getValueByField("mseMap");
            mseEventContent = (Map<String, Object>) mseEvent.get("StreamingNotification");
            location = (Map<String, Object>) mseEventContent.get("location");
            Map<String, Object> druid = new HashMap<>();

            if (location != null) {
                mapInfo = (Map<String, Object>) location.get("MapInfo");
                if (mapInfo == null) {
                    mapInfo = (Map<String, Object>) location.get("mapInfo");
                }

                String mapHierachy = (String) mapInfo.get("mapHierarchyString");
                if (mapHierachy != null) {
                    String[] zone = mapHierachy.split(">");
                    String wirelessStation = (String) location.get("apMacAddress");

                    if(wirelessStation==null){
                        wirelessStation = "unknown";
                    }

                   // System.out.println("CACHE: " + cacheLocation);
                    if (!cacheLocation.isEmpty()) {
                        String oldFloor = (String) cacheLocation.get("client_floor");
                        String oldBuilding = (String) cacheLocation.get("client_building");
                        String oldCampus = (String) cacheLocation.get("client_campus");
                        String oldwirelessStation = (String) cacheLocation.get("wireless_station");

                        if (oldFloor != null)
                            if (!oldFloor.equals(zone[2])) {
                                druid.put("client_floor_old", oldFloor);
                                druid.put("client_floor_new", zone[2]);
                            } else {
                                druid.put("client_floor", zone[2]);
                            }

                        if (oldwirelessStation != null)
                            if (!oldwirelessStation.equals(wirelessStation)) {
                                druid.put("wireless_station_old", oldwirelessStation);
                                druid.put("wireless_station_new", wirelessStation);
                            } else {
                                druid.put("wireless_station", wirelessStation);
                            }

                        if (oldBuilding != null)
                            if (!oldBuilding.equals(zone[1])) {
                                druid.put("client_building_old", oldBuilding);
                                druid.put("client_building_new", zone[1]);
                            } else {
                                druid.put("client_building", zone[1]);
                            }

                        if (oldCampus != null)
                            if (!oldCampus.equals(zone[0])) {
                                druid.put("client_campus_old", oldCampus);
                                druid.put("client_campus_new", zone[0]);
                            } else {
                                druid.put("client_campus", zone[0]);
                            }

                    } else {
                        druid.put("client_floor_new", zone[2]);
                        druid.put("client_campus_new", zone[0]);
                        druid.put("client_building_new", zone[1]);
                    }
                }

                String macAddress = (String) location.get("macAddress");
                String timestamp = (String) mseEvent.get("timestamp");

                druid.put("sensor_name", mseEventContent.get("subscriptionName"));
                druid.put("timestamp", new DateTime(timestamp).withZone(DateTimeZone.UTC).getMillis() / 1000);
                druid.put("client_mac", macAddress);
                tridentCollector.emit(new Values(druid));
            }
        }
    }
}
