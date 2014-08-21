/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>This function analyzes the trap events and get interest fields.</p>
 * @author Andres Gomez
 */
public class GetTRAPdata extends BaseFunction {

    /**
     * <p>This function analyzes the trap events and get interest fields.</p>
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> rssi = (Map<String, Object>) tuple.getValue(0);

        Object macAuxObject = rssi.get(".1.3.6.1.4.1.9.9.599.1.2.32.0");
        Object clientRssiObject = rssi.get(".1.3.6.1.4.1.9.9.599.1.2.1.0");


        try {

            if (macAuxObject != null && clientRssiObject != null) {

                Integer rssiInt = (Integer) clientRssiObject;

                String macAux = macAuxObject.toString();
                String macAddress = macAux.split("/")[1];

                Map<String, Object> rssiData = new HashMap<>();

                if (rssiInt <= -90)
                    rssiData.put("client_rssi", "bad");
                else if (rssiInt <= -80)
                    rssiData.put("client_rssi", "low");
                else if (rssiInt <= -70)
                    rssiData.put("client_rssi", "medium");
                else if (rssiInt <= -60)
                    rssiData.put("client_rssi", "good");
                else if (rssiInt <= -50)
                    rssiData.put("client_rssi", "excelent");
                else if (rssiInt == 0)
                    rssiData.put("client_rssi", "unknown");

                collector.emit(new Values(macAddress, rssiData));
            }
        } catch (NullPointerException e) {
            Logger.getLogger(GetTRAPdata.class.getName()).log(Level.SEVERE, "Failed reading a TRAP JSON tuple: \n" + rssi.toString(), e);
        }
    }

}
