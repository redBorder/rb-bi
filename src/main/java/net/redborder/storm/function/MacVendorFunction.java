/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * Get MacVendor from mac address (source and destination).
 *
 * @author andresgomez
 */
public class MacVendorFunction extends BaseFunction {

    private final String _ouiFilePath = "/opt/rb/etc/objects/oui-vendors";
    Map<String, String> _ouiMap;
    boolean debug;
    
    public MacVendorFunction(boolean debug){
        this.debug=debug;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        InputStream in = null;
        _ouiMap = new HashMap<>();

        try {
            in = new FileInputStream(_ouiFilePath);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MacVendorFunction.class.getName()).log(Level.SEVERE, null, ex);
        }

        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(isr);

        try {
            String line = br.readLine();

            while (line != null) {
                String[] tokens = line.split("\\|");
                _ouiMap.put(tokens[0].substring(2, 8), tokens[1]);
                line = br.readLine();
            }
        } catch (IOException ex) {
            Logger.getLogger(MacVendorFunction.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Create the oui from the mac address.
     *
     * @param object Mac address.
     * @return oui.
     */
    private String buildOui(Object object) {
        String mac = object.toString();
        mac = mac.trim().replace("-", "").replace(":", "");
        return mac.substring(0, 6).toUpperCase();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        Map<String, Object> vendorMap = new HashMap<>();
        String clientMac = (String) event.get("client_mac");

        if (clientMac != null) {
            String oui = buildOui(clientMac);
            vendorMap.put("client_mac_vendor", _ouiMap.get(oui));
        }

        collector.emit(new Values(vendorMap));
    }
}