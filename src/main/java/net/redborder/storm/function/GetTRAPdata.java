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
            if (rssi.containsKey(".1.3.6.1.4.1.9.9.599.1.2.32.0") && rssi.containsKey(".1.3.6.1.4.1.9.9.599.1.2.1.0")) {
                String macAux = rssi.get(".1.3.6.1.4.1.9.9.599.1.2.32.0").toString();
                String macAddress = macAux.split("/")[1];

                Map<String, Object> rssiData = new HashMap<>();

                rssiData.put("client_rssi", rssi.get(".1.3.6.1.4.1.9.9.599.1.2.1.0"));

                collector.emit(new Values(macAddress, rssiData));
            }
        } catch (NullPointerException e) {
            Logger.getLogger(GetTRAPdata.class.getName()).log(Level.SEVERE, "Failed reading a TRAP JSON tuple: \n" +rssi.toString(), e);
        }
    }

}
