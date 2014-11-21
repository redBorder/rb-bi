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
public class GetLocationState extends BaseFunction {

    Map<String, Object> mseEventContent, location, statistics, mapInfo;

    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        Map<String, Object> mseEvent = (Map<String, Object>) tuple.getValueByField("mse_map");
        if (mseEvent != null) {
            Map<String, Object> cacheLocation = (Map<String, Object>) tuple.getValueByField("mseMap");
            mseEventContent = (Map<String, Object>) mseEvent.get("StreamingNotification");
            location = (Map<String, Object>) mseEventContent.get("location");
            Map<String, Object> state = new HashMap<>();

            if (location != null) {
                mapInfo = (Map<String, Object>) location.get("MapInfo");
                if (mapInfo == null) {
                    mapInfo = (Map<String, Object>) location.get("mapInfo");
                }

                String mapHierachy = (String) mapInfo.get("mapHierarchyString");
                if (mapHierachy != null) {
                    String[] zone = mapHierachy.split(">");

                    if (!cacheLocation.isEmpty()) {
                        String oldFloor = (String) cacheLocation.get("client_floor");
                        String oldBuilding = (String) cacheLocation.get("client_building");
                        String oldCampus = (String) cacheLocation.get("client_campus");

                        if (oldFloor != null)
                            if (!oldFloor.equals(zone[2])) {
                                state.put("client_floor_old", oldFloor);
                                state.put("client_floor_new", zone[2]);
                            }

                        if (oldBuilding != null)
                            if (!oldBuilding.equals(zone[1])) {
                                state.put("client_building_old", oldBuilding);
                                state.put("client_building_new", zone[1]);
                            }

                        if (oldCampus != null)
                            if (!oldCampus.equals(zone[0])) {
                                state.put("client_campus_old", oldCampus);
                                state.put("client_campus_new", zone[0]);
                            }

                    }else{
                        state.put("client_floor_new", zone[2]);
                        state.put("client_campus_new", zone[0]);
                        state.put("client_building_new", zone[1]);
                    }
                    state.put("client_floor", zone[2]);
                    state.put("client_campus", zone[0]);
                    state.put("client_building", zone[1]);
                }

                String macAddress = (String) location.get("macAddress");

/*
                statistics = (Map<String, Object>) location.get("Statistics");
                if (statistics != null) {

                    String firstLocatedTime = (String) statistics.get("firstLocatedTime");
                    Long firstTime = new DateTime(firstLocatedTime).withZone(DateTimeZone.UTC).getMillis() / 60000;

                    String lastLocatedTime = (String) statistics.get("lastLocatedTime");
                    Long lastTime = new DateTime(lastLocatedTime).withZone(DateTimeZone.UTC).getMillis() / 60000;


                    while (lastTime <= firstTime) {

                        state.put("timestamp", lastTime * 60);
                        state.put("client_mac", macAddress.hashCode());
                        state.put("state", 1);
                        lastTime++;
                        tridentCollector.emit(new Values(state));

                        if (state.containsKey("client_floor_new"))
                            state.remove("client_floor_new");
                        if (state.containsKey("client_floor_old"))
                            state.remove("client_floor_old");
                    }
                }

*/
                String timestamp = (String) mseEvent.get("timestamp");

                state.put("sensor_name", mseEventContent.get("subscriptionName"));
                state.put("timestamp", new DateTime(timestamp).withZone(DateTimeZone.UTC).getMillis() / 1000);
                state.put("client_mac", macAddress);
                state.put("state", 1);
                tridentCollector.emit(new Values(state));
            }
        }
    }
}
