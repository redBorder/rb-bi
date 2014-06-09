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

    private String _ouiFilePath;

    Map<String, String> _ouiMap;

    /**
     * Constructor.
     *
     * @param ouiPath Path where is the database of oui.
     */
    public MacVendorFunction(String ouiPath) {
        _ouiFilePath = ouiPath;
    }

    public MacVendorFunction() {
        _ouiFilePath = "/opt/rb/etc/objects/oui-vendors";
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        Map<String, Object> vendorMap = new HashMap<>();
        String ouiSrc = "";
        if (event.containsKey("client_mac")) {
            ouiSrc = buildOui(event.get("client_mac"));

            if (ouiSrc != null) {
                ouiSrc = _ouiMap.get(ouiSrc);
                if (ouiSrc != null) {
                    vendorMap.put("client_mac_vendor", ouiSrc);
                }
            }
        }
        collector.emit(new Values(vendorMap));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _ouiMap = new HashMap<>();

        InputStream in = null;

        try {
            in = new FileInputStream(_ouiFilePath);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MacVendorFunction.class.getName()).log(Level.SEVERE, null, ex);
        }

        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(isr);

        while (true) {
            String line = null;
            try {
                line = br.readLine();

            } catch (IOException ex) {
                Logger.getLogger(MacVendorFunction.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (line == null) {
                break;
            }

            String[] tokens = line.split("\\|");
            _ouiMap.put(tokens[0].substring(2, 8), tokens[1]);

        }
    }

    /**
     * Create the oui from the mac address.
     *
     * @param object Mac address.
     * @return oui.
     */
    private String buildOui(Object object) {
        if (object != null) {
            String mac = object.toString();
            mac = mac.trim().replace("-", "").replace(":", "");
            return mac.substring(0, 6).toUpperCase();
        } else {
            return "";
        }
    }
}
