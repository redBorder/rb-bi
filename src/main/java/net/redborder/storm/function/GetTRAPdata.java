/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class GetTRAPdata extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> rssi = (Map<String, Object>) tuple.getValue(0);

        try {
            String macAux = rssi.get("iso.3.6.1.4.1.9.9.599.1.2.32.0").toString();
            String macAddress = macAux.split("/")[1];

            Map<String, Object> rssiData = new HashMap<>();

            rssiData.put("rssi", rssi.get("iso.3.6.1.4.1.9.9.599.1.2.1.0"));
            //rssiData.put("location_floor", rssi.get("SNMPv2-SMI-v1::enterprises.9.9.513.1.1.1.1.49.0"));

            collector.emit(new Values(macAddress, rssiData));
        } catch (NullPointerException e) {
            Logger.getLogger(GetTRAPdata.class.getName()).log(Level.SEVERE, "Failed reading a TRAP JSON tuple", e);
        }
    }

}
