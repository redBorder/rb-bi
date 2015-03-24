package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 22/1/15.
 */
public class LocationLogicNmsp extends BaseFunction {

    final Long MINUTE = 60L;
    final Long CLIENT_LEAVE_TIME = 30L * MINUTE;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> locationCache = (Map<String, Object>) tuple.getValueByField("nmsp_location_state_cache");
        Map<String, Object> location = (Map<String, Object>) tuple.getValueByField("nmsp_location_state");

        Map<String, Object> state = new HashMap<>();
        Map<String, Object> druid = new HashMap<>();

        String newFloor = (String) location.get("client_floor");
        String newBuilding = (String) location.get("client_building");
        String newCampus = (String) location.get("client_campus");
        String newZone = (String) location.get("client_zone");

        if(newFloor == null)
           newFloor = "unknown";

        if(newBuilding == null)
            newBuilding = "unknown";

        if(newCampus == null)
            newCampus = "unknown";

        if(newZone == null)
            newZone = "unknown";

        String sensorName = (String) location.get("sensor_name");
        String wirelessStation = (String) location.get("wireless_station");

        String clientMac = (String) location.get("client_mac");

       // System.out.println("CLIENT_MAC: " + clientMac);

        Long newTimestamp = (Long) location.get("timestamp");

        boolean moved = false;
        Long oldTimestamp = 0L;

        if (!locationCache.isEmpty()) {
            String oldFloor = (String) locationCache.get("client_floor");
            String oldBuilding = (String) locationCache.get("client_building");
            String oldCampus = (String) locationCache.get("client_campus");
            String oldwirelessStation = (String) locationCache.get("wireless_station");
            String oldZone = (String) locationCache.get("client_zone");

            // Dwell Time

            oldTimestamp = Long.valueOf(locationCache.get("old_timestamp").toString());

            if (newTimestamp - oldTimestamp > MINUTE && newTimestamp - oldTimestamp <= CLIENT_LEAVE_TIME) {
         //       System.out.println("RECUPERANDO: " + (newTimestamp - oldTimestamp)/MINUTE);
                for (long i = 1; i * MINUTE < (newTimestamp - oldTimestamp - MINUTE*2); i++) {
                    Long timestamp = oldTimestamp + i * MINUTE;
            //        System.out.println("[ " + i + " ]: " + timestamp);
                    druid.put("timestamp", timestamp);
                    druid.put("client_mac", clientMac);
                    druid.put("sensor_name", sensorName);
                    druid.put("type", "nmsp");

                    if (oldFloor != null)
                        druid.put("client_floor", oldFloor);
                    if (oldCampus != null)
                        druid.put("client_campus", oldCampus);
                    if (oldBuilding != null)
                        druid.put("client_building", oldBuilding);
                    if (oldZone != null)
                        druid.put("client_zone", oldZone);

                    druid.put("wireless_station", oldwirelessStation);
                    collector.emit(new Values(null, druid));
                }
            }

            // Location moved

            druid.clear();

            if (oldFloor != null)
                if (!oldFloor.equals(newFloor)) {
                    druid.put("client_floor_old", oldFloor);
                    druid.put("client_floor_new", newFloor);
                    moved = true;
                } else {
                    druid.put("client_floor", newFloor);
                }

            if (oldZone != null)
                if (!oldZone.equals(newZone)) {
                    druid.put("client_zone_old", oldZone);
                    druid.put("client_zone_new", newZone);
                    moved = true;
                } else {
                    druid.put("client_zone", newZone);
                }

            if (oldwirelessStation != null)
                if (!oldwirelessStation.equals(wirelessStation)) {
                    druid.put("wireless_station_old", oldwirelessStation);
                    druid.put("wireless_station_new", wirelessStation);
                    moved = true;
                } else {
                    druid.put("wireless_station", wirelessStation);
                }

            if (oldBuilding != null)
                if (!oldBuilding.equals(newBuilding)) {
                    druid.put("client_building_old", oldBuilding);
                    druid.put("client_building_new", newBuilding);
                    moved = true;
                } else {
                    druid.put("client_building", newBuilding);
                }

            if (oldCampus != null)
                if (!oldCampus.equals(newCampus)) {
                    druid.put("client_campus_old", oldCampus);
                    druid.put("client_campus_new", newCampus);
                    moved = true;
                } else {
                    druid.put("client_campus", newCampus);
                }

        } else {
            if (newFloor != null)
                druid.put("client_floor_new", newFloor);
            if (newCampus != null)
                druid.put("client_campus_new", newCampus);
            if (newBuilding != null)
                druid.put("client_building_new", newBuilding);
            if (newZone != null)
                druid.put("client_zone_new", newZone);
        }

        // End

        state.put("old_timestamp", newTimestamp);

        if (newFloor != null)
            state.put("client_floor", newFloor);
        if (newCampus != null)
            state.put("client_campus", newCampus);
        if (newBuilding != null)
            state.put("client_building", newBuilding);
        if (newZone != null)
            state.put("client_zone", newZone);

        state.put("wireless_station", wirelessStation);

        druid.put("sensor_name", sensorName);
        druid.put("type", "nmsp");
        druid.put("client_mac", clientMac);
        druid.put("wireless_station", wirelessStation);

        //System.out.println("MOVED: " + moved);

       //System.out.println("DIFF: " +(newTimestamp - oldTimestamp));
        if (moved && newTimestamp - oldTimestamp <= MINUTE){
            //System.out.println("TIMESTAMP: " + (newTimestamp));
            druid.put("timestamp", newTimestamp);
            collector.emit(new Values(state, druid));
        }else if(newTimestamp - oldTimestamp > MINUTE){
            //System.out.println("TIMESTAMP: " + (newTimestamp));
            druid.put("timestamp", newTimestamp);
            collector.emit(new Values(state, druid));
        }

    }
}
